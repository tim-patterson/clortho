use crate::varint::{write_varint_signed, write_varint_unsigned};
use std::io::{Seek, SeekFrom, Write};

mod records;
mod varint;

/// Generic trait for writing into blocks
pub trait KVWritable {
    fn write_key(&self, buffer: &mut Vec<u8>);
    fn write_value(&self, buffer: &mut Vec<u8>);
    fn partition_hash(&self) -> u16;
}

/// Component used to write blocks
#[derive(Default)]
pub struct BlockBuilder {
    buffer: Vec<u8>,
    // Key start, Key end, value length
    pointers: Vec<(u32, u32, u8)>,
}

impl BlockBuilder {
    /// Writes a record into the buffer
    pub fn write_record<R: KVWritable>(&mut self, record: &R) {
        let start_pointer = self.buffer.len();
        record.write_key(&mut self.buffer);
        let key_end = self.buffer.len();
        record.write_value(&mut self.buffer);
        self.buffer
            .extend_from_slice(record.partition_hash().to_be_bytes().as_ref());
        let end_pointer = self.buffer.len();
        self.pointers.push((
            start_pointer as u32,
            key_end as u32,
            (end_pointer - key_end) as u8,
        ));
    }

    /// Returns the size in bytes of the data
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Flush all the data out to a local file.
    pub fn flush<W: Write + Seek>(&mut self, writer: &mut W) -> Result<(), std::io::Error> {
        let buffer = &self.buffer;
        self.pointers
            .sort_unstable_by(|(start1, end1, _), (start2, end2, _)| {
                let a = &buffer[(*start1 as usize)..(*end1 as usize)];
                let b = &buffer[(*start2 as usize)..(*end2 as usize)];
                a.cmp(b)
            });

        // // record the offsets of every 16th row.
        // let mut pointers = Vec::with_capacity(self.pointers.len() / 16 + 1);
        // let mut varint_buf: Vec<u8> = Vec::with_capacity(8);
        //
        // for (idx, (k_start, k_end, v_len)) in self.pointers.iter().enumerate() {
        //
        //     // The records in the block should be written in the following way
        //     // In the context of a tuple record the payload and the timestamp make up the
        //     // key while the frequency and partition hash make up the value.
        //     // key_length: varint,
        //     // value_length: u8,
        //     // payload: bytes[key_length-8]
        //     // timestamp: 8 bytes big endian(inverted)
        //     // frequency: varint
        //     // partition_hash: 2 bytes big endian.
        //     // This can roughly be thought of as framing/key/value.
        //
        //     // The b+tree part of the file only has pointers into every 16th record
        //     if idx % 16 == 0 {
        //         let key = &buffer[(*k_start as usize)..(*k_end as usize)];
        //         pointers.push((key, writer.seek(SeekFrom::Current(0)).unwrap() as u32));
        //     }
        //
        //     // Key len
        //     varint_buf.clear();
        //     write_varint_unsigned(*k_end - *k_start, &mut varint_buf);
        //     writer.write_all(&varint_buf)?;
        //     // Value len
        //     writer.write_all(&[*v_len])?;
        //     // Contents
        //     writer.write_all(&buffer[(*k_start as usize)..(*k_end as usize + v_len as usize)])
        // }
        //
        // // Now we write out the b+tree
        // // Each page is as follows
        // // pivot_count: u8
        // // pivot_pointers: [u32; pivot_count]
        // // child_pointers: [u32; pivot_count + 1]
        // // pivots:
        // //   key_length: varint
        // //   key_bytes: bytes[key_length]
        // for chunk  in pointers.chunks(16) {
        //     // Pivot count
        //     writer.write_all(&[chunk.len() as u8 -1])?;
        //     for window in chunk.windows(2) {
        //         let (left_bytes, _) = window[0];
        //         let (right_bytes, _)  = window[1];
        //
        //     }
        // }
        //

        Ok(())
    }
}
