use crate::file_store::{FileStore, Writable};
use memmap::Mmap;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::panicking;

/// File store managing files on disk,
/// For reading we use pooled mmap'd files.
pub struct LocalFileStore {
    data_directory: PathBuf,
    open_files: RwLock<HashMap<String, Arc<MmapInner>>>,
}

impl LocalFileStore {
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        LocalFileStore {
            data_directory: data_directory.as_ref().to_path_buf(),
            open_files: RwLock::new(HashMap::new()),
        }
    }
}

impl FileStore for LocalFileStore {
    type W = LocalFileStoreWriter;
    type R = LocalFileStoreReader;

    fn open_for_write(&self, identifier: &str) -> std::io::Result<Self::W> {
        let file = OpenOptions::new()
            .truncate(true)
            .write(true)
            .create(true)
            .open(self.data_directory.join(identifier))?;

        let writer = LocalFileStoreWriter {
            file,
            flushed: false,
        };
        Ok(writer)
    }

    fn open_for_read(&self, identifier: &str) -> std::io::Result<Self::R> {
        // First check to see if we already have the file mmapped somewhere
        {
            let open_files = self.open_files.read().unwrap();
            if let Some(shared) = open_files.get(identifier) {
                return Ok(LocalFileStoreReader(Arc::clone(shared)));
            }
        }
        // Nah we have to actually create the mmap, do the double read lock thing...
        let mut open_files = self.open_files.write().unwrap();
        if let Some(shared) = open_files.get(identifier) {
            return Ok(LocalFileStoreReader(Arc::clone(shared)));
        }

        let path = self.data_directory.join(identifier);
        let file = File::open(&path)?;
        let mem_view = Arc::new(MmapInner {
            mmap: Some(unsafe { memmap::Mmap::map(&file) }?),
            path,
            delete: AtomicBool::from(false),
        });
        open_files.insert(identifier.to_string(), Arc::clone(&mem_view));
        Ok(LocalFileStoreReader(mem_view))
    }

    fn delete(&self, identifier: &str) -> std::io::Result<()> {
        // Here we don't actually delete but just set a delete flag instead.
        let mut open_files = self.open_files.write().unwrap();
        if let Some(existing) = open_files.remove(identifier) {
            existing.delete.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Wrapper around File so we can track and assert that flush/fsync etc is being called
/// properly
pub struct LocalFileStoreWriter {
    file: File,
    flushed: bool,
}

impl Write for LocalFileStoreWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.file.write_all(buf)
    }
}

impl Seek for LocalFileStoreWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Writable for LocalFileStoreWriter {
    fn flush_and_close(mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        self.flushed = true;
        Ok(())
    }
}

/// Just here as a check to make sure that the rest of the code base does the right thing
impl Drop for LocalFileStoreWriter {
    fn drop(&mut self) {
        if !self.flushed && !panicking() {
            panic!("File dropped without being flushed")
        }
    }
}

/// Wrapper around Memmap to get deref working properly
pub struct LocalFileStoreReader(Arc<MmapInner>);

impl Deref for LocalFileStoreReader {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.mmap.as_ref().unwrap().as_ref()
    }
}

/// Wrapper around our mmap inners that allow us to delay deletion until
/// our last read reference is dropped.  Needed for running on windows.
struct MmapInner {
    // Only unset during drop
    mmap: Option<Mmap>,
    path: PathBuf,
    delete: AtomicBool,
}

impl Drop for MmapInner {
    fn drop(&mut self) {
        if *self.delete.get_mut() {
            // unlink mmap and then delete.
            self.mmap = None;
            std::fs::remove_file(&self.path).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;

    #[test]
    fn test_file_block_store() {
        let file_path = "../../target/file_store";
        std::fs::remove_dir_all(file_path).ok();
        std::fs::create_dir_all(file_path).unwrap();

        let block_store = LocalFileStore::new(file_path);

        let mut writer = block_store.open_for_write("foobar").unwrap();
        writer.write_all(b"hello").unwrap();
        writer.write_all(b"world").unwrap();
        writer.flush_and_close().unwrap();

        {
            // we should be able to open the file_store for reading now, multiple times even
            let reader1 = block_store.open_for_read("foobar").unwrap();
            let reader2 = block_store.open_for_read("foobar").unwrap();
            assert_eq!(b"helloworld".as_ref(), reader1.deref());
            assert_eq!(b"helloworld".as_ref(), reader2.deref());

            // Now delete
            block_store.delete("foobar").unwrap();

            // But already open readers should still be able to be read
            assert_eq!(b"helloworld".as_ref(), reader1.deref());
        }
        // But once the readers have dropped their references the file should be GC'd and removed
        // from disk
        assert!(block_store.open_for_read("foobar").is_err());
    }
}
