pub mod sst_buffered_writer;
pub mod sst_reader;
pub mod sst_writer;

/// Metadata about an sst file
pub struct SstInfo {
    pub min_record: Box<[u8]>,
    pub max_record: Box<[u8]>,
    pub size: u32,
}
