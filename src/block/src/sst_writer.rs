use crate::{KVWritable, MergeFunction};
use std::io::{Write, Seek, SeekFrom};
use std::time::SystemTime;
use crate::varint::write_varint_unsigned;
use std::cmp::min;

/// Writer to write out sst files.
#[derive(Default)]
pub struct SstWriter {
    buffer: Vec<u8>,
    // Key start, Key end, Value end
    data_pointers: Vec<(u32, u32, u32)>,
}

/// Internal struct used to pass around the info about sub trees when
/// building the btree structures.
struct PageData<'a> {
    min: &'a [u8],
    max: &'a [u8],
    pointer: i32,
}

// Number of pointers/children in each b+tree page.
// Should be a power of 2 to get optimal balanced binary search
const SEARCH_TREE_SIZE: usize = 64;
// At what interval the search tree hooks into the data section.
// Our seeks have to linear scan through up to this many records
// and things like prefix compression would be reset at these intervals.
const LOWER_LEAF_SIZE: usize = 16;

impl SstWriter {
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
        SstWriter::write_header(writer)?;
        let pages = SstWriter::write_data(&self.buffer, &mut self.data_pointers, writer)?;
        let tree_pointer = SstWriter::write_search_tree(&pages, writer)?;
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        SstWriter::write_footer(timestamp, tree_pointer, writer)?;

        Ok(())
    }

    /// Writes the block header
    fn write_header<W: Write + Seek>(writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(
            b"clortho
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

        let mut page_datas = Vec::with_capacity(pointers.len() / LOWER_LEAF_SIZE + 1);

        // TODO we actually need to do some magic here and merge duplicates if we expect this to be
        // able to be used directly.
        panic!();
        for chunk in pointers.chunks(LOWER_LEAF_SIZE) {
            let (min_start, min_key_end, _) = chunk.first().unwrap();
            let (max_start, max_key_end, _) = chunk.last().unwrap();
            let min_key = &buffer[(*min_start as usize)..(*min_key_end as usize)];
            let max_key = &buffer[(*max_start as usize)..(*max_key_end as usize)];

            let pointer = -(writer.seek(SeekFrom::Current(0)).unwrap() as i32);

            for data_pointer in chunk {
                SstWriter::write_record(buffer, data_pointer, writer)?;
            }

            page_datas.push(PageData {
                min: min_key,
                max: max_key,
                pointer,
            });
        }
        writer.write_all(b"\0\0\0\0")?;

        Ok(page_datas)
    }

    fn write_record<W: Write + Seek>(
        buffer: &[u8],
        data_pointer: &(u32, u32, u32),
        writer: &mut W,
    ) -> Result<(), std::io::Error> {
        // Key length
        write_varint_unsigned(data_pointer.1 - data_pointer.0, writer)?;

        // Value length, minus 2 for the shard_prefix
        write_varint_unsigned(data_pointer.2 - data_pointer.1 - 2, writer)?;

        // Key/Value/Shard_prefix
        writer.write_all(&buffer[(data_pointer.0 as usize)..(data_pointer.2 as usize)])?;
        Ok(())
    }

    /// Writes the search tree portion of the block, returns the "pointer" into the root node of the
    /// tree (or directly into the data if there's <=16 records in the file)
    fn write_search_tree<W: Write + Seek>(
        children: &[PageData],
        writer: &mut W,
    ) -> Result<i32, std::io::Error> {
        if let [child] = children {
            return Ok(child.pointer);
        } else if children.is_empty() {
            panic!("We can't write a search tree for 0 items...")
        }

        let mut child_pages = Vec::with_capacity(children.len() / SEARCH_TREE_SIZE + 1);

        for chunk in children.chunks(SEARCH_TREE_SIZE) {
            // Write pivots
            let pivot_pointers = chunk
                .windows(2)
                .map::<Result<_, std::io::Error>, _>(|left_right| {
                    let left_val = left_right[0].max;
                    let right_val = left_right[1].min;
                    let pivot = &right_val[..(common_prefix_len(left_val, right_val))];
                    let pointer = writer.seek(SeekFrom::Current(0)).unwrap() as i32;
                    write_varint_unsigned(pivot.len() as u32, writer)?;
                    writer.write_all(pivot)?;
                    Ok(pointer)
                })
                .collect::<Result<Vec<_>, _>>()?;

            let page_pointer = writer.seek(SeekFrom::Current(0)).unwrap() as i32;
            // Write child count
            writer.write_all(&[chunk.len() as u8])?;
            // Write pivot pointers
            for pointer in pivot_pointers {
                writer.write_all(pointer.to_be_bytes().as_ref())?;
            }
            // Write child pointers
            for child in chunk {
                writer.write_all(child.pointer.to_be_bytes().as_ref())?;
            }

            child_pages.push(PageData {
                min: chunk.first().unwrap().min,
                max: chunk.last().unwrap().max,
                pointer: page_pointer,
            })
        }

        // Recurse...
        SstWriter::write_search_tree(&child_pages, writer)
    }

    /// Writes the block header
    fn write_footer<W: Write + Seek>(timestamp: u64, tree_pointer: i32, writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(timestamp.to_be_bytes().as_ref())?;
        writer.write_all(tree_pointer.to_be_bytes().as_ref())?;
        writer.write_all(1_u16.to_be_bytes().as_ref())
    }
}

/// Returns the length in bytes of the common prefix of two byte arrays
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    for (idx, (a, b)) in a.iter().zip(b).enumerate() {
        if *a != *b {
            return idx - 1;
        }
    }
    min(a.len(), b.len())
}
