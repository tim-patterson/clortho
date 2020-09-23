use std::io::Write;
use utils::streaming_iter::StreamingIter;
use utils::Timestamp;

pub mod file_store;
pub mod records;
pub mod sst;

/// Trait to be implemented for records to be written out, allows serializing
/// directly into output buffer/file_store etc.
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
