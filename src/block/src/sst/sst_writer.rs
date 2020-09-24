use std::cmp::min;
use std::io::{Seek, SeekFrom, Write};
use utils::varint::write_varint_unsigned;

// We're making the sst writer push based rather than pull(iterator) based under the assumption
// that this will allow more flexibility in the higher layers rather than forcing everything above
// into a chains of iterators...

/// Writer for a single SST file.
/// This writer is rather low level and expects the data to be written to it in sorted order
/// with no duplicates
/// See https://github.com/tim-patterson/clortho/blob/master/docs/FILE_FORMAT.md
/// for the file_store format produced by this writer.
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
    min: Vec<u8>,
    max: Vec<u8>,
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
    /// Creates a new Sst Writer, the file_store header will be eagerly
    /// be written at this point.
    pub fn new(writer: W) -> std::io::Result<Self> {
        let mut sst_writer = SstWriter {
            writer,
            data_pages: vec![],
            page_offset: 0,
            current_page: PageData::default(),
        };
        sst_writer.write_header()?;
        Ok(sst_writer)
    }

    /// Returns the size in bytes of the file_store so far,
    /// Can be polled to assess the size of the file_store when
    /// deciding the chunk the output.
    /// This only the size of the header + data section.
    /// On file_store close/flush we'll write the btree and footer sections.
    pub fn size(&mut self) -> usize {
        self.writer.seek(SeekFrom::Current(0)).unwrap() as usize
    }

    /// Pushes a record into the low-level storage, at this point we expect the timestamp to be
    /// appended onto the record_key as u64 BE.
    pub fn push_record(&mut self, record_key: &[u8], record_value: &[u8]) -> std::io::Result<i32> {
        // Unfortunately we must know the record_key/value length upfront so we can't use the
        // KVWriteable interface, however in the future a KVWriteableWithLen might be an optimization
        // that could work in some cases.
        let record_pointer = -(self.size() as i32);
        write_varint_unsigned(record_key.len() as u32, &mut self.writer)?;
        write_varint_unsigned(record_value.len() as u32, &mut self.writer)?;
        self.writer.write_all(record_key)?;
        self.writer.write_all(record_value)?;
        // Update page data min(if start of page), max
        if self.page_offset == 0 {
            self.current_page.min = record_key.to_vec();
            self.current_page.pointer = record_pointer;
        }
        self.current_page.max.clear();
        self.current_page.max.extend_from_slice(record_key);

        self.page_offset += 1;
        if self.page_offset == LOWER_LEAF_SIZE {
            self.page_offset = 0;
            self.data_pages.push(std::mem::take(&mut self.current_page));
        }
        Ok(record_pointer)
    }

    /// Writes the search tree portion of the block, returns the "pointer" into the root node of the
    /// tree (or directly into the data if there's <=16 records in the file_store)
    fn write_search_tree(mut children: Vec<PageData>, writer: &mut W) -> std::io::Result<i32> {
        // Terminal condition.
        if let [child] = children.as_slice() {
            return Ok(child.pointer);
        } else if children.is_empty() {
            panic!("We can't write a search tree for 0 items...")
        }

        let mut child_pages = Vec::with_capacity(children.len() / SEARCH_TREE_SIZE + 1);

        for chunk in children.chunks_mut(SEARCH_TREE_SIZE) {
            // If the chunk only has one child we call just pass up the whole page
            // if there was 17 records we would hit this case for example. The result is simply that
            // some pointers may skip over a layer instead of pointing to a pivotless page.
            if let [page] = chunk {
                child_pages.push(std::mem::take(page))
            }

            // Write pivots
            let pivot_pointers = chunk
                .windows(2)
                .map::<Result<_, std::io::Error>, _>(|left_right| {
                    let left_val = left_right[0].max.as_ref();
                    let right_val = left_right[1].min.as_ref();
                    // The common prefix + 1 extra char from the right side is all that's required
                    // to truncate the prefix down to the minimal possible size
                    let pivot = &right_val[..(common_prefix_len(left_val, right_val) + 1)];
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
            for child in chunk.iter() {
                writer.write_all(child.pointer.to_be_bytes().as_ref())?;
            }

            let min_tuple = std::mem::take(&mut chunk.first_mut().unwrap().min);
            let max_tuple = std::mem::take(&mut chunk.first_mut().unwrap().max);

            child_pages.push(PageData {
                min: min_tuple,
                max: max_tuple,
                pointer: page_pointer,
            })
        }

        // Recurse...
        SstWriter::write_search_tree(child_pages, writer)
    }

    /// Let the writer know that we're done with the writes,
    /// at this point the writer can write any needed indexes etc
    pub fn finish(mut self) -> std::io::Result<W> {
        // Copy across current page.
        if self.page_offset != 0 {
            self.data_pages.push(std::mem::take(&mut self.current_page))
        }

        let root_pointer = if self.data_pages.is_empty() {
            // Special case for an empty
            let p = -(self.size() as i32);
            // Write the terminator record.
            self.writer.write_all(&[0, 0])?;
            p
        } else {
            // Write the terminator record.
            self.writer.write_all(&[0, 0])?;
            let pages = std::mem::take(&mut self.data_pages);
            SstWriter::write_search_tree(pages, &mut self.writer)?
        };

        self.write_footer(root_pointer)?;

        Ok(self.writer)
    }

    /// Writes the block header
    fn write_header(&mut self) -> Result<(), std::io::Error> {
        self.writer.write_all(
            b"clortho
data
v1






---
",
        )
    }

    /// Writes the block header
    fn write_footer(&mut self, tree_pointer: i32) -> std::io::Result<()> {
        self.writer.write_all(tree_pointer.to_be_bytes().as_ref())?;
        // Version
        self.writer.write_all(1_u16.to_be_bytes().as_ref())
    }
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
        let mut sst_writer = SstWriter::new(Cursor::new(vec![]))?;
        assert_eq!(sst_writer.size(), HEADER_SIZE);

        let output = sst_writer.finish()?.into_inner();
        assert_eq!(&output[..HEADER_SIZE], EXPECTED_HEADER);
        assert_eq!(
            &output[HEADER_SIZE..],
            [
                0_u8, 0, // Terminator record
                255, 255, 255, 230, // Data pointer
                0, 1 // File version
            ]
            .as_ref()
        );
        Ok(())
    }

    #[test]
    fn test_sst_writer_with_records() -> Result<(), Box<dyn Error>> {
        let rec_1_key_ts = [1_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1];
        let rec_1_value = [5_u8];
        let rec_2_key_ts = [2_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1];
        let rec_2_value = [6_u8];

        let mut sst_writer = SstWriter::new(Cursor::new(vec![]))?;
        sst_writer.push_record(&rec_1_key_ts, &rec_1_value)?;
        sst_writer.push_record(&rec_2_key_ts, &rec_2_value)?;

        assert_eq!(
            &sst_writer.finish()?.into_inner()[HEADER_SIZE..],
            [
                10_u8, 1_u8, // key, value lengths
                1_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1,    // key/ts
                5_u8, // value
                10_u8, 1_u8, // key, value lengths
                2_u8, 2, 0, 0, 0, 0, 0, 0, 0, 1,    // key/ts
                6_u8, // value,
                0_u8, 0, // Terminator record
                255, 255, 255, 230, // data pointer
                0, 1 // File version
            ]
            .as_ref()
        );
        Ok(())
    }

    #[test]
    fn test_sst_writer_with_tree() -> Result<(), Box<dyn Error>> {
        // We need at least 17 records to trigger the btree to build
        let mut sst_writer = SstWriter::new(Cursor::new(vec![]))?;
        for i in 0..17 {
            let rec_key_ts = [i as u8, 0, 0, 0, 0, 0, 0, 0, 0];
            let rec_value = [i as u8];
            sst_writer.push_record(&rec_key_ts, &rec_value)?;
        }

        let mut expected_data = vec![];
        for i in 0..17 {
            expected_data.push(9_u8); // key size
            expected_data.push(1_u8); // value size
            expected_data.extend_from_slice([i as u8, 0, 0, 0, 0, 0, 0, 0, 0].as_ref()); // key_ts
            expected_data.push(i as u8); // value
        }
        let end_of_data = HEADER_SIZE + expected_data.len();

        let data = sst_writer.finish()?.into_inner();

        // Check data section
        assert_eq!(&data[HEADER_SIZE..end_of_data], expected_data.as_slice());

        // Check btree and footer
        // Here we'd expect a 1 layer btree with 1 node containing 1 pivot and 2 children.
        assert_eq!(
            &data[end_of_data..],
            [
                0_u8, 0, // Terminator record
                1, 16, // Our pivot (len, bytes)
                2,  // Child count -- This is where the footer should point to.
                0, 0, 0, 232, // Pointer back to the first pivot
                255, 255, 255, 230, // Child pointer to the start of the data block
                255, 255, 255, 38, // pointer to data block 16 records later (16 * 12b = 192)
                0, 0, 0, 234, // Pointer to the child count
                0, 1 // File version
            ]
        );
        Ok(())
    }
}
