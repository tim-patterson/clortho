use std::io::{Seek, Write};
use std::ops::Deref;

pub mod local_file_store;
pub mod memory_file_store;

/// A Filesystem abstraction for writing/reading blocks. Allows us to swap on-disk with in-memory
/// and even remote stores, as well as provide wrappers for caching etc.
/// Written files are immutable once written.
pub trait FileStore {
    type W: Writable + 'static;
    type R: Deref<Target = [u8]> + 'static;
    type E;

    /// Returns a writer for a writing a new block
    fn open_for_write(&self, identifier: &str) -> Result<Self::W, Self::E>;

    /// Opens a block for reading
    fn open_for_read(&self, identifier: &str) -> Result<Self::R, Self::E>;

    /// Marks a block as able to be deleted, the delete should only happen
    /// once existing references to this block are dropped.
    fn delete(&self, identifier: &str) -> Result<(), Self::E>;
}

pub trait Writable: Write + Seek {
    /// Flushes, fsyncs and closes the file, should be used instead of letting drop close
    /// the file as errors will be lost if doing that
    fn flush_and_close(self) -> std::io::Result<()>;
}
