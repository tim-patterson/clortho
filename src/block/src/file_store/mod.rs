use std::io::{Seek, Write};
use std::ops::Deref;

pub mod memory_file_store;

/// A Filesystem abstraction for writing/reading blocks. Allows us to swap on-disk with in-memory
/// and even remote stores, as well as provide wrappers for caching etc.
/// Written files are immutable once written.
pub trait FileStore {
    type W: Write + Seek + 'static;
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
