use crate::varint::write_varint_signed;
use crate::KVWritable;

/// What we store for a typical table update.
#[derive(Debug, Clone)]
pub struct TupleRecord {
    // The data itself
    pub payload: Vec<u8>,
    // The logical timestamp of this delta
    pub timestamp: u64,
    // The frequency for this delta(ie add/remove)
    pub frequency: i64,
    // The hash used to bucket data into n partitions if needed
    pub shard_hash: u16,
}

impl KVWritable for TupleRecord {
    fn write_key(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.payload);
        buffer.extend_from_slice((u64::MAX - self.timestamp).to_be_bytes().as_ref());
    }

    fn write_value(&self, buffer: &mut Vec<u8>) {
        write_varint_signed(self.frequency, buffer);
    }

    fn shard_hash(&self) -> u16 {
        self.shard_hash
    }
}
