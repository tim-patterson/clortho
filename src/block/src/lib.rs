use crate::varint::write_varint_unsigned;
use std::io::{Seek, SeekFrom, Write};

mod records;
mod varint;

/// Generic trait for writing into blocks
pub trait KVWritable {
    fn write_key(&self, buffer: &mut Vec<u8>);
    fn write_value(&self, buffer: &mut Vec<u8>);
    fn shard_hash(&self) -> u16;
}

struct PageData<'a> {
    min: &'a [u8],
    max: &'a [u8],
    pointer: i32,
}

/// Component used to write blocks
#[derive(Default)]
pub struct BlockBuilder {
    buffer: Vec<u8>,
    // Key start, Key end, Value end
    data_pointers: Vec<(u32, u32, u32)>,
}

impl BlockBuilder {
    /// Writes a record into the buffer
    pub fn append<R: KVWritable>(&mut self, record: &R) {
        let start_pointer = self.buffer.len();
        record.write_key(&mut self.buffer);
        let key_end = self.buffer.len();
        record.write_value(&mut self.buffer);
        self.buffer
            .extend_from_slice(record.shard_hash().to_be_bytes().as_ref());
        let end_pointer = self.buffer.len();
        self.data_pointers
            .push((start_pointer as u32, key_end as u32, end_pointer as u32));
    }

    /// Returns the size in bytes of the data
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Flush all the data out to a local file.
    pub fn flush<W: Write + Seek>(&mut self, writer: &mut W) -> Result<(), std::io::Error> {
        self.write_header(writer)?;
        let _page_datas = BlockBuilder::write_data(&self.buffer, &mut self.data_pointers, writer)?;

        Ok(())
    }

    /// Writes the block header
    fn write_header<W: Write + Seek>(&mut self, writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(
            b"cloud storage
data
v1






---
",
        )
    }

    /// Writes out the data, returns the the needed data to build the next layer of the btree
    fn write_data<'a, W: Write + Seek>(
        buffer: &'a [u8],
        pointers: &mut Vec<(u32, u32, u32)>,
        writer: &mut W,
    ) -> Result<Vec<PageData<'a>>, std::io::Error> {
        // Sort the data
        pointers.sort_unstable_by(|(start1, end1, _), (start2, end2, _)| {
            let a = &buffer[(*start1 as usize)..(*end1 as usize)];
            let b = &buffer[(*start2 as usize)..(*end2 as usize)];
            a.cmp(b)
        });

        let mut page_datas = Vec::with_capacity(pointers.len() / 16 + 1);

        for chunk in pointers.chunks(16) {
            let (min_start, min_key_end, _) = chunk.first().unwrap();
            let (max_start, max_key_end, _) = chunk.last().unwrap();
            let min_key = &buffer[(*min_start as usize)..(*min_key_end as usize)];
            let max_key = &buffer[(*max_start as usize)..(*max_key_end as usize)];

            let pointer = -(writer.seek(SeekFrom::Current(0)).unwrap() as i32);

            for data_pointer in chunk {
                BlockBuilder::write_record(buffer, &mut vec![], data_pointer, writer)?;
            }

            page_datas.push(PageData {
                min: min_key,
                max: max_key,
                pointer,
            });
        }

        Ok(page_datas)
    }

    fn write_record<W: Write + Seek>(
        buffer: &[u8],
        varint_buf: &mut Vec<u8>,
        data_pointer: &(u32, u32, u32),
        writer: &mut W,
    ) -> Result<(), std::io::Error> {
        // Key length
        varint_buf.clear();
        write_varint_unsigned(data_pointer.1 - data_pointer.0, varint_buf);
        writer.write_all(varint_buf)?;

        // Value length
        varint_buf.clear();
        // Minus 2 for the shard_prefix
        write_varint_unsigned(data_pointer.2 - data_pointer.1 - 2, varint_buf);
        writer.write_all(varint_buf)?;

        // Key/Value/Shard_prefix
        writer.write_all(&buffer[(data_pointer.0 as usize)..(data_pointer.2 as usize)])?;
        Ok(())
    }
}
