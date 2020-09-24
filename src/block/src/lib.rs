use std::io::Write;

pub mod file_store;
pub mod merge;
pub mod records;
pub mod sst;

/// Trait to be implemented for records to be written out, allows serializing
/// directly into output buffers in some cases
pub trait KVWritable {
    fn write_key<W: Write>(&self, buffer: &mut W) -> std::io::Result<()>;
    fn write_value<W: Write>(&self, buffer: &mut W) -> std::io::Result<()>;
}

/// Default implementation for passing through kv tuples of bytes
impl KVWritable for (&[u8], &[u8]) {
    fn write_key<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        buffer.write_all(self.0)
    }

    fn write_value<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        buffer.write_all(self.1)
    }
}
