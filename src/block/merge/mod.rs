use crate::utils::streaming_iter::StreamingKVIter;

/// Trait to be implemented for merging multiple records together, this is used to remove duplicates
/// when appending data into a block, when reading data from multiple files and for compactions.
pub trait Merger {
    /// Wraps a stream of sorted records and returns a new stream with duplicates removed.
    /// This interface is allot more complex than the rocksdb one, but this gives us the power
    /// to take into account the surrounding records which is handy when we're storing multiple
    /// versions for a record etc.
    fn merge<'a, I: StreamingKVIter<K = [u8], V = [u8], E = std::io::Error> + 'a>(
        &self,
        iter: I,
    ) -> Box<dyn StreamingKVIter<K = [u8], V = [u8], E = std::io::Error> + 'a>;
}

/// A Dummy Merger that just does nothing
pub struct NoopMerger {}

impl Merger for NoopMerger {
    fn merge<'a, I: StreamingKVIter<K = [u8], V = [u8], E = std::io::Error> + 'a>(
        &self,
        iter: I,
    ) -> Box<dyn StreamingKVIter<K = [u8], V = [u8], E = std::io::Error> + 'a> {
        Box::from(iter)
    }
}
