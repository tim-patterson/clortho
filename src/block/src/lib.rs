use std::io::{Seek, Write};
use std::ops::Deref;
use utils::streaming_iter::StreamingIter;
use utils::Timestamp;

pub mod memory_file_store;
pub mod records;
pub mod sst_writer;

/// Trait to be implemented for records to be written out, allows serializing
/// directly into output buffer/file etc.
pub trait KVWritable {
    fn write_key<W: Write>(&self, buffer: &mut W) -> std::io::Result<()>;
    fn write_value<W: Write>(&self, buffer: &mut W) -> std::io::Result<()>;
    /// Timestamp should be set to zero if its not used.
    fn timestamp(&self) -> Timestamp;
}

/// Trait to be implemented for merging multiple records together, this is used to remove duplicates
/// when appending data into a block, when reading data from multiple files and for compactions.
pub trait MergeFunction {
    /// Accepts the input for multiple keys and merges them together.
    /// Merged output should be written into `merged`.
    /// Return type is keep, ie return false to throw skip this value if it's been merged to nothing.
    /// When dealing with multi-versioned data(ie timestamps) this will be called with data from
    /// newest to oldest.
    fn merge<I: StreamingIter<I = [u8], E = std::io::Error>, W: Write>(
        iter: &mut I,
        merged: &mut W,
    ) -> std::io::Result<bool>;

    /// Used to signal that we want to treat the data as absolutes not deltas, while we could do
    /// that just by using merge, this provides the performance optimization of not needing to walk
    /// lower blocks if we find a match higher up.
    fn latest_only() -> bool {
        false
    }
}

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
