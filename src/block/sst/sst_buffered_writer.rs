use crate::block::file_store::Writable;
use crate::block::merge::Merger;
use crate::block::sst::sst_writer::SstWriter;
use crate::block::sst::SstInfo;
use crate::block::KVWritable;
use crate::utils::streaming_iter;

/// A Wrapper around the raw sst writer that allows us to write the data out
/// in any order we want, simply buffering and then sorting when finishing,
/// We need a merger to allow us to combine duplicate keys before flushing
pub struct SstBufferedWriter<W: Writable, M: Merger> {
    inner: SstWriter<W>,
    // Buffer of raw KV bytes
    bytes_buffer: Vec<u8>,
    // Sorted list of pointers (start_offset, key_end_offset, value_end_offset)
    pointers: Vec<(u32, u32, u32)>,
    merger: M,
}

impl<W: Writable, M: Merger> SstBufferedWriter<W, M> {
    /// Creates a new buffered writer for the give file
    pub fn new(writer: W, merger: M) -> std::io::Result<Self> {
        let inner = SstWriter::new(writer)?;
        Ok(SstBufferedWriter {
            inner,
            bytes_buffer: vec![],
            pointers: vec![],
            merger,
        })
    }

    /// Pushs a record into the buffer
    pub fn push_record<R: KVWritable>(&mut self, record: R) -> std::io::Result<()> {
        // We could just unwrap instead of throwing io errors as writing into a vec will never
        // error, but lets bubble up the results incase we ever decide to spill to disk
        let start_offset = self.bytes_buffer.len() as u32;
        record.write_key(&mut self.bytes_buffer)?;
        let key_end_offset = self.bytes_buffer.len() as u32;
        record.write_value(&mut self.bytes_buffer)?;
        let value_end_offset = self.bytes_buffer.len() as u32;
        self.pointers
            .push((start_offset, key_end_offset, value_end_offset));
        Ok(())
    }

    /// Let the writer know that we're done with the all the records and to write everything
    /// out to storage
    pub fn finish(mut self) -> std::io::Result<SstInfo> {
        // Sort the pointers
        let buffer = &self.bytes_buffer;
        self.pointers
            .sort_by(|(start1, end1, _), (start2, end2, _)| {
                let a = &buffer[(*start1 as usize)..(*end1 as usize)];
                let b = &buffer[(*start2 as usize)..(*end2 as usize)];
                a.cmp(b)
            });
        // Write into the underlying writer
        let kv_iter = streaming_iter::wrap(self.pointers.into_iter().map(
            |(start_offset, key_end_offset, value_end_offset)| {
                (
                    &buffer[(start_offset as usize)..(key_end_offset as usize)],
                    &buffer[(key_end_offset as usize)..(value_end_offset as usize)],
                )
            },
        ));

        let mut merged = self.merger.merge(kv_iter);

        while let Some((k, v)) = merged.next()? {
            self.inner.push_record(k, v)?;
        }

        self.inner.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::merge::NoopMerger;
    use crate::block::sst::sst_reader::SstReader;
    use std::error::Error;
    use std::io::Cursor;

    #[test]
    fn test_sst_writer() -> Result<(), Box<dyn Error>> {
        let merger = NoopMerger {};
        let mut output = Cursor::new(vec![]);
        let mut sst_writer = SstBufferedWriter::new(&mut output, merger)?;
        // We're testing that we can write out of order but when we read the file everything is
        // sorted
        sst_writer.push_record((b"c".as_ref(), b"2".as_ref()))?;
        sst_writer.push_record((b"a".as_ref(), b"1".as_ref()))?;
        sst_writer.push_record((b"e".as_ref(), b"3".as_ref()))?;
        let sst_info = sst_writer.finish()?;

        let mut reader = SstReader::new(output.into_inner());

        assert_eq!(sst_info.min_record.as_ref(), b"a".as_ref());
        assert_eq!(sst_info.max_record.as_ref(), b"e".as_ref());

        reader.seek(b"");
        assert_eq!(reader.get(), Some((b"a".as_ref(), b"1".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), Some((b"c".as_ref(), b"2".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), Some((b"e".as_ref(), b"3".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), None);
        Ok(())
    }
}
