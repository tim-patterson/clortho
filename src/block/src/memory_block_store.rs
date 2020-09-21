use crate::BlockStore;
use std::collections::HashMap;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

/// In memory block store
#[derive(Debug, Default)]
pub struct MemoryBlockStore {
    map: Arc<RwLock<HashMap<String, Arc<[u8]>>>>,
}

impl BlockStore for MemoryBlockStore {
    type W = MemoryBlockStoreWriter;
    type R = Arc<[u8]>;
    type E = ();

    fn open_for_write(&self, identifier: &str) -> Result<Self::W, Self::E> {
        let writer = MemoryBlockStoreWriter {
            buffer: Cursor::new(vec![]),
            identifier: identifier.to_string(),
            map: Arc::clone(&self.map),
        };
        Ok(writer)
    }

    fn open_for_read(&self, identifier: &str) -> Result<Self::R, Self::E> {
        self.map
            .read()
            .unwrap()
            .get(identifier)
            .map(Arc::clone)
            .ok_or(())
    }

    fn delete(&self, identifier: &str) -> Result<(), Self::E> {
        self.map.write().unwrap().remove(identifier);
        Ok(())
    }
}

/// Wrapper around vec, holds a reference back to the block store's internal map,
/// when it goes out of scope(ie the write is finished), we'll add it to the block
/// store and it will be avaliable for reads.
pub struct MemoryBlockStoreWriter {
    buffer: Cursor<Vec<u8>>,
    identifier: String,
    map: Arc<RwLock<HashMap<String, Arc<[u8]>>>>,
}

impl Write for MemoryBlockStoreWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.buffer.write_all(buf)
    }
}

impl Seek for MemoryBlockStoreWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.buffer.seek(pos)
    }
}

impl Drop for MemoryBlockStoreWriter {
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer).into_inner();
        let identifier = std::mem::take(&mut self.identifier);
        self.map
            .write()
            .unwrap()
            .insert(identifier, Arc::from(buffer));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;

    #[test]
    fn test_memory_block_store() {
        let block_store = MemoryBlockStore::default();
        {
            let mut writer = block_store.open_for_write("foobar").unwrap();
            writer.write_all(b"hello").unwrap();
            writer.write_all(b"world").unwrap();
            // A read should give us nothing while the writer is in scope
            assert!(block_store.open_for_read("foobar").is_err());
        }
        // we should be able to open the file for reading now, multiple times even
        let reader1 = block_store.open_for_read("foobar").unwrap();
        let reader2 = block_store.open_for_read("foobar").unwrap();
        assert_eq!(b"helloworld".as_ref(), reader1.deref());
        assert_eq!(b"helloworld".as_ref(), reader2.deref());

        // Now delete
        block_store.delete("foobar").unwrap();
        assert!(block_store.open_for_read("foobar").is_err());

        // But already open readers should still be able to be read
        assert_eq!(b"helloworld".as_ref(), reader1.deref());
    }
}
