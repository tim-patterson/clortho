use crate::{KVWritable, MergeFunction};
use std::io::Write;
use utils::streaming_iter::StreamingIter;
use utils::varint::{read_varint_signed, write_varint_signed};
use utils::Timestamp;

/// A record type that is simply a key -> i64,
/// writes are treated as diff's and the merge/compaction simply collapses them together.
#[derive(Debug, Clone)]
pub struct CounterRecord<'a> {
    // The data itself
    pub key: &'a [u8],
    // The logical timestamp of this delta
    pub timestamp: Timestamp,
    // The frequency for this delta(ie add/remove)
    pub delta: i64,
}

impl<'a> CounterRecord<'a> {
    /// Creates a new record using the current timestamp
    pub fn new(key: &'a [u8], delta: i64) -> Self {
        CounterRecord {
            key,
            timestamp: Timestamp::now(),
            delta,
        }
    }
}

impl KVWritable for CounterRecord<'_> {
    fn write_key<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        buffer.write_all(&self.key)
    }

    fn write_value<W: Write>(&self, buffer: &mut W) -> std::io::Result<()> {
        write_varint_signed(self.delta, buffer)
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

impl MergeFunction for CounterRecord<'_> {
    fn merge<I: StreamingIter<I = [u8], E = std::io::Error>, W: Write>(
        iter: &mut I,
        merged: &mut W,
    ) -> std::io::Result<bool> {
        let mut freq = 0_i64;
        let mut tmp = 0_i64;
        while let Some(f) = iter.next()? {
            read_varint_signed(&mut tmp, f);
            freq += tmp;
        }
        if freq == 0 {
            Ok(false)
        } else {
            write_varint_signed(freq, merged)?;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utils::streaming_iter::wrap;
    use utils::varint::VARINT_SIGNED_ZERO_ENC;

    #[test]
    fn test_record_write() {
        let record = CounterRecord::new(b"abcd", 4);
        let mut key_buffer = vec![];
        let mut value_buffer = vec![];
        record.write_key(&mut key_buffer).unwrap();
        record.write_value(&mut value_buffer).unwrap();

        assert_eq!(b"abcd".as_ref(), key_buffer.as_slice());
        assert_eq!(
            [VARINT_SIGNED_ZERO_ENC + 4].as_ref(),
            value_buffer.as_slice()
        );
    }

    #[test]
    fn test_record_merge_keep() {
        let mut output = vec![];
        let values = vec![
            [VARINT_SIGNED_ZERO_ENC + 4].as_ref(),
            [VARINT_SIGNED_ZERO_ENC + 2].as_ref(),
        ];
        let mut iter = wrap(values.into_iter());
        let keep = CounterRecord::merge(&mut iter, &mut output).unwrap();

        assert!(keep);
        assert_eq!([VARINT_SIGNED_ZERO_ENC + 6].as_ref(), output.as_slice());
    }

    #[test]
    fn test_record_merge_zero() {
        let mut output = vec![];
        let values = vec![
            [VARINT_SIGNED_ZERO_ENC + 4].as_ref(),
            [VARINT_SIGNED_ZERO_ENC - 4].as_ref(),
        ];
        let mut iter = wrap(values.into_iter());
        let keep = CounterRecord::merge(&mut iter, &mut output).unwrap();

        assert_eq!(false, keep);
    }
}
