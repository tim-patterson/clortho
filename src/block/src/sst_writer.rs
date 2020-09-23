use std::cmp::min;
use std::io::{Seek, SeekFrom, Write};
use utils::varint::write_varint_unsigned;

// We're making the sst writer push based rather than pull(iterator) based under the assumption
// that this will allow more flexibility in the higher layers rather than forcing everything above
// into a chains of iterators...

/// Writer for a single SST file.
/// This writer is rather low level and expects the data to be written to it in sorted order
/// with no duplicates
/// See https://github.com/tim-patterson/clortho/blob/master/docs/BLOCK_FORMAT.md
/// for the file format produced by this writer.
pub struct SstWriter<W: Write + Seek> {
    writer: W,
    // List of low-level data pages.
    data_pages: Vec<PageData>,
    // page_offset - the *next* index for the current page,
    // ie a value between 0 and 15
    page_offset: usize,
    // The data for the current page.
    current_page: PageData,
}

/// Internal struct used to pass around the info about sub trees when
/// building the btree structures.
#[derive(Default, Clone, Debug)]
struct PageData {
    min: Box<[u8]>,
    max: Box<[u8]>,
    pointer: i32,
}

// Number of pointers/children in each b+tree page.
// Should be a power of 2 to get optimal balanced binary search
const SEARCH_TREE_SIZE: usize = 64;
// At what interval the search tree hooks into the data section.
// Our seeks have to linear scan through up to this many records
// and things like prefix compression would be reset at these intervals.
const LOWER_LEAF_SIZE: usize = 16;

impl<W: Write + Seek> SstWriter<W> {
    /// Creates a new Sst Writer, the file header will be eagerly
    /// be written at this point.
    pub fn new(mut writer: W) -> std::io::Result<Self> {
        SstWriter::write_header(&mut writer)?;
        Ok(SstWriter {
            writer,
            data_pages: vec![],
            page_offset: 0,
            current_page: PageData::default(),
        })
    }

    /// Returns the size in bytes of the file so far,
    /// Can be polled to assess the size of the file when
    /// deciding the chunk the output.
    /// This only the size of the header + data section.
    /// On file close/flush we'll write the btree and footer sections.
    pub fn size(&mut self) -> usize {
        self.writer.seek(SeekFrom::Current(0)).unwrap() as usize
    }

    /// Pushes a record into the low-level storage, at this point we expect the timestamp to be
    /// appended onto the record_key as u64 BE.
    pub fn push_record(
        &mut self,
        record_key_ts: &[u8],
        record_value: &[u8],
    ) -> std::io::Result<i32> {
        let record_pointer = -(self.size() as i32);
        write_varint_unsigned(record_key_ts.len() as u32 - 8, &mut self.writer)?;
        self.writer.write_all(record_key_ts)?;
        write_varint_unsigned(record_value.len() as u32, &mut self.writer)?;
        self.writer.write_all(record_value)?;
        // Update page data
        if self.page_offset == 0 {
            self.current_page.min = Box::from(record_key_ts);
            self.current_page.pointer = record_pointer;
        }
        self.page_offset += 1;
        if self.page_offset == LOWER_LEAF_SIZE {
            self.current_page.max = Box::from(record_key_ts);
            self.page_offset = 0;
            self.data_pages.push(std::mem::take(&mut self.current_page));
        }
        Ok(record_pointer)
    }

    // /// Flush all the data out to a local file.
    // pub fn flush<W: Write + Seek>(&mut self, writer: &mut W) -> Result<(), std::io::Error> {
    //     let pages = SstWriter::write_data(&self.buffer, &mut self.data_pointers, writer)?;
    //     let tree_pointer = SstWriter::write_search_tree(&pages, writer)?;
    //     let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
    //     SstWriter::write_footer(timestamp, tree_pointer, writer)?;
    //
    //     Ok(())
    // }

    /// Writes the block header
    fn write_header(writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(
            b"clortho
data
v1






---
",
        )
    }

    // /// Writes out the data, returns the the needed data to build the next layer of the btree
    // fn write_data<'a, W: Write + Seek>(
    //     buffer: &'a [u8],
    //     pointers: &mut Vec<(u32, u32, u32)>,
    //     writer: &mut W,
    // ) -> Result<Vec<PageData<'a>>, std::io::Error> {
    //     // Sort the data
    //     pointers.sort_unstable_by(|(start1, end1, _), (start2, end2, _)| {
    //         let a = &buffer[(*start1 as usize)..(*end1 as usize)];
    //         let b = &buffer[(*start2 as usize)..(*end2 as usize)];
    //         a.cmp(b)
    //     });
    //
    //     let mut page_datas = Vec::with_capacity(pointers.len() / LOWER_LEAF_SIZE + 1);
    //
    //     // TODO we actually need to do some magic here and merge duplicates if we expect this to be
    //     // able to be used directly.
    //     panic!();
    //     for chunk in pointers.chunks(LOWER_LEAF_SIZE) {
    //         let (min_start, min_key_end, _) = chunk.first().unwrap();
    //         let (max_start, max_key_end, _) = chunk.last().unwrap();
    //         let min_key = &buffer[(*min_start as usize)..(*min_key_end as usize)];
    //         let max_key = &buffer[(*max_start as usize)..(*max_key_end as usize)];
    //
    //         let pointer = -(writer.seek(SeekFrom::Current(0)).unwrap() as i32);
    //
    //         for data_pointer in chunk {
    //             SstWriter::write_record(buffer, data_pointer, writer)?;
    //         }
    //
    //         page_datas.push(PageData {
    //             min: min_key,
    //             max: max_key,
    //             pointer,
    //         });
    //     }
    //     writer.write_all(b"\0\0\0\0")?;
    //
    //     Ok(page_datas)
    // }
    //
    // fn write_record<W: Write + Seek>(
    //     buffer: &[u8],
    //     data_pointer: &(u32, u32, u32),
    //     writer: &mut W,
    // ) -> Result<(), std::io::Error> {
    //     // Key length
    //     write_varint_unsigned(data_pointer.1 - data_pointer.0, writer)?;
    //
    //     // Value length, minus 2 for the shard_prefix
    //     write_varint_unsigned(data_pointer.2 - data_pointer.1 - 2, writer)?;
    //
    //     // Key/Value/Shard_prefix
    //     writer.write_all(&buffer[(data_pointer.0 as usize)..(data_pointer.2 as usize)])?;
    //     Ok(())
    // }
    //
    // /// Writes the search tree portion of the block, returns the "pointer" into the root node of the
    // /// tree (or directly into the data if there's <=16 records in the file)
    // fn write_search_tree<W: Write + Seek>(
    //     children: &[PageData],
    //     writer: &mut W,
    // ) -> Result<i32, std::io::Error> {
    //     if let [child] = children {
    //         return Ok(child.pointer);
    //     } else if children.is_empty() {
    //         panic!("We can't write a search tree for 0 items...")
    //     }
    //
    //     let mut child_pages = Vec::with_capacity(children.len() / SEARCH_TREE_SIZE + 1);
    //
    //     for chunk in children.chunks(SEARCH_TREE_SIZE) {
    //         // Write pivots
    //         let pivot_pointers = chunk
    //             .windows(2)
    //             .map::<Result<_, std::io::Error>, _>(|left_right| {
    //                 let left_val = left_right[0].max;
    //                 let right_val = left_right[1].min;
    //                 let pivot = &right_val[..(common_prefix_len(left_val, right_val))];
    //                 let pointer = writer.seek(SeekFrom::Current(0)).unwrap() as i32;
    //                 write_varint_unsigned(pivot.len() as u32, writer)?;
    //                 writer.write_all(pivot)?;
    //                 Ok(pointer)
    //             })
    //             .collect::<Result<Vec<_>, _>>()?;
    //
    //         let page_pointer = writer.seek(SeekFrom::Current(0)).unwrap() as i32;
    //         // Write child count
    //         writer.write_all(&[chunk.len() as u8])?;
    //         // Write pivot pointers
    //         for pointer in pivot_pointers {
    //             writer.write_all(pointer.to_be_bytes().as_ref())?;
    //         }
    //         // Write child pointers
    //         for child in chunk {
    //             writer.write_all(child.pointer.to_be_bytes().as_ref())?;
    //         }
    //
    //         child_pages.push(PageData {
    //             min: chunk.first().unwrap().min,
    //             max: chunk.last().unwrap().max,
    //             pointer: page_pointer,
    //         })
    //     }
    //
    //     // Recurse...
    //     SstWriter::write_search_tree(&child_pages, writer)
    // }
    //
    // /// Writes the block header
    // fn write_footer<W: Write + Seek>(timestamp: u64, tree_pointer: i32, writer: &mut W) -> Result<(), std::io::Error> {
    //     writer.write_all(timestamp.to_be_bytes().as_ref())?;
    //     writer.write_all(tree_pointer.to_be_bytes().as_ref())?;
    //     writer.write_all(1_u16.to_be_bytes().as_ref())
    // }
}

/// Returns the length in bytes of the common prefix of two byte arrays,
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    for (idx, (a, b)) in a.iter().zip(b).enumerate() {
        if *a != *b {
            return idx;
        }
    }
    min(a.len(), b.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io::Cursor;

    const EXPECTED_HEADER: &[u8] = b"clortho
data
v1






---
";
    const HEADER_SIZE: usize = 26;

    #[test]
    fn test_common_prefix_len() {
        // Same value
        assert_eq!(3, common_prefix_len(b"abc", b"abc"));
        // One empty
        assert_eq!(0, common_prefix_len(b"", b"abc"));
        // One superset
        assert_eq!(3, common_prefix_len(b"abcd", b"abc"));
        // Diverging
        assert_eq!(3, common_prefix_len(b"abcd", b"abce"));
        // Nothing in common
        assert_eq!(0, common_prefix_len(b"abcd", b"efgh"));
    }

    #[test]
    fn test_sst_writer_empty() -> Result<(), Box<dyn Error>> {
        let mut buf = Cursor::new(vec![]);
        {
            let mut sst_writer = SstWriter::new(&mut buf)?;
            assert_eq!(sst_writer.size(), HEADER_SIZE);
        }

        assert_eq!(buf.into_inner().as_slice(), EXPECTED_HEADER);
        Ok(())
    }

    #[test]
    fn test_sst_writer_with_records() -> Result<(), Box<dyn Error>> {
        let rec_1_key_ts = [1_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1];
        let rec_1_value = [5_u8];
        let rec_2_key_ts = [2_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1];
        let rec_2_value = [6_u8];

        let mut buf = Cursor::new(vec![]);
        {
            let mut sst_writer = SstWriter::new(&mut buf)?;
            sst_writer.push_record(&rec_1_key_ts, &rec_1_value)?;
            sst_writer.push_record(&rec_2_key_ts, &rec_2_value)?;
        }

        assert_eq!(
            &buf.into_inner()[HEADER_SIZE..],
            [
                2_u8, 1_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1, // key_len and key/ts
                1_u8, 5_u8, // value_len and value
                2_u8, 2_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1, // key_len and key/ts
                1_u8, 6_u8, // value_len and value
            ]
            .as_ref()
        );
        Ok(())
    }
}
