use crate::utils::varint::read_varint_unsigned;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::Deref;

/// Reader that can read an sst file
/// See https://github.com/tim-patterson/clortho/blob/master/docs/FILE_FORMAT.md
/// for the file_store format parsed by this reader.
/// Conceptually the reader is like a (streaming) iterator where the current position can
/// be moved around.
/// As this does no IO its infallible so wont ever throw in the StreamingKVIter interface,
/// so we allow the error type to be specified by the caller to align with other interfaces as needed
pub struct SstReader<D: Deref<Target = [u8]>, E = std::io::Error> {
    data: D,
    // The position of the *next* record.
    // Static isn't the right lifetime as its really a slice out of data but we can't do
    // that in rust..., we could pass around usizes like pointers but its allot of mess and
    // we'd end up paying for a whole bunch more bounds checking than we really need
    next_position: Option<&'static [u8]>,
    key_value: Option<(&'static [u8], &'static [u8])>,
    _p: PhantomData<E>,
}

impl<D: Deref<Target = [u8]>> SstReader<D> {
    /// Creates a new sst reader
    pub fn new(data: D) -> Self {
        SstReader {
            data,
            next_position: None,
            key_value: None,
            _p: PhantomData::default(),
        }
    }

    /// Seeks to the first record with a key equal to or greater than the given key
    pub fn seek(&mut self, key: &[u8]) {
        let data_len = self.data.len();
        let pointer = i32::from_be_bytes(
            self.data[(data_len - 6)..(data_len - 2)]
                .as_ref()
                .try_into()
                .unwrap(),
        );
        self.next_position = self.walk_from(pointer, key);
    }

    /// Advances to the next record
    pub fn advance(&mut self) {
        // Really advance shouldn't be called unless there is a next position...
        if let Some(mut buffer) = self.next_position {
            let mut key_len = 0;
            let mut val_len = 0;
            buffer = read_varint_unsigned(&mut key_len, buffer);
            buffer = read_varint_unsigned(&mut val_len, buffer);
            // We've run off the end of the data
            if key_len == 0 && val_len == 0 {
                self.key_value = None;
                self.next_position = None;
            } else {
                let key_data = &buffer[..(key_len as usize)];
                let value_data = &buffer[(key_len as usize)..((key_len + val_len) as usize)];
                self.key_value = Some((key_data, value_data));
                self.next_position = Some(&buffer[((key_len + val_len) as usize)..]);
            }
        } else {
            self.key_value = None;
        }
    }

    /// Returns the data at the current position
    pub fn get(&self) -> Option<(&[u8], &[u8])> {
        self.key_value
    }

    fn walk_from(&mut self, from: i32, key: &[u8]) -> Option<&'static [u8]> {
        if from < 0 {
            // negative means we're a pointer to the data section.
            let ptr = (-from) as usize;
            // We always keep this slice aligned to the start of the record,
            // Transmute to make this static.
            let mut buffer =
                unsafe { std::mem::transmute::<&[u8], &'static [u8]>(&self.data[ptr..]) };
            loop {
                let mut key_len = 0;
                let mut val_len = 0;
                buffer = read_varint_unsigned(&mut key_len, buffer);
                buffer = read_varint_unsigned(&mut val_len, buffer);
                // We've run off the end of the data
                if key_len == 0 && val_len == 0 {
                    self.key_value = None;
                    return None;
                }
                let key_data = &buffer[..(key_len as usize)];

                // We've found a match
                if key_data >= key {
                    let value_data = &buffer[(key_len as usize)..((key_len + val_len) as usize)];
                    self.key_value = Some((key_data, value_data));
                    return Some(&buffer[((key_len + val_len) as usize)..]);
                } else {
                    buffer = &buffer[((key_len + val_len) as usize)..];
                }
            }
        } else {
            // We're in the btree nodes...
            let child_count = self.data[(from as usize)];
            let pivot_ptr_base = from as usize + 1_usize;
            let child_ptr_base = (child_count - 1) as usize * 4 + pivot_ptr_base;
            let child_idx = binary_search(child_count, |pivot_idx| {
                // We need to index into the pivot pointers(each 4 bytes long)
                // use that to grab the pivot which is length prefixed
                let pivot_ptr_ptr = pivot_idx as usize * 4 + pivot_ptr_base;
                let pointer_bytes = &self.data[pivot_ptr_ptr..(pivot_ptr_ptr + 4)];
                let pivot_pointer = u32::from_be_bytes(pointer_bytes.try_into().unwrap()) as usize;
                let mut pivot_buffer = &self.data[pivot_pointer..];
                let mut pivot_len = 0;
                pivot_buffer = read_varint_unsigned(&mut pivot_len, pivot_buffer);

                pivot_buffer[..(pivot_len as usize)].cmp(key)
            });

            let child_ptr_ptr = child_idx as usize * 4 + child_ptr_base;
            let child_ptr = i32::from_be_bytes(
                self.data[child_ptr_ptr..(child_ptr_ptr + 4)]
                    .as_ref()
                    .try_into()
                    .unwrap(),
            );

            self.walk_from(child_ptr, key)
        }
    }
}

/// A custom binary search that instead of working on a slice like that
/// of the standard library simply works on a usize that is an index into
/// something else.
/// We want to treat == mid the same as > mid as thats the way our pivots work
/// f should compare <being_searched>.cmp(<search_key>)
/// size here returns to the "children", ie one more than the number of pivots
fn binary_search<F>(size: u8, mut f: F) -> u8
where
    F: FnMut(u8) -> Ordering,
{
    // Narrows in on left, right
    let mut left = 0_u8;
    let mut right = size - 1;
    while right != left {
        let mid = (left + right) / 2;
        let cmp = f(mid);
        // Arggg this stuff does my head in, all these are equiv.
        // if search_key >= pivot then <high> else <low>
        // if pivot <= search_key then <high> else <low>
        // if pivot > search_key then <low> else <high>
        if cmp == Ordering::Greater {
            // if left = 0, right = 2, mid = 1
            // when calling cmp for 1, its really the pivot between 1 and 2.
            // based on the narrowed ranges are either (0-1) or (2-2)
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::sst::sst_writer::SstWriter;
    use std::error::Error;
    use std::io::Cursor;

    #[test]
    fn test_binary_search() -> Result<(), Box<dyn Error>> {
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&-5)), 0);
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&0)), 1);
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&5)), 1);
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&10)), 2);
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&15)), 2);
        assert_eq!(binary_search(3, |idx| (idx as i32 * 10).cmp(&20)), 2);
        Ok(())
    }

    #[test]
    fn test_sst_reader_empty() -> Result<(), Box<dyn Error>> {
        let mut output = Cursor::new(vec![]);
        let sst_writer = SstWriter::new(&mut output)?;
        sst_writer.finish()?;

        let mut reader = SstReader::new(output.into_inner());

        reader.seek(b"1");
        assert_eq!(reader.get(), None);
        Ok(())
    }

    #[test]
    fn test_sst_reader_no_btree() -> Result<(), Box<dyn Error>> {
        let mut output = Cursor::new(vec![]);
        let mut sst_writer = SstWriter::new(&mut output)?;
        sst_writer.push_record(b"a", b"1")?;
        sst_writer.push_record(b"c", b"2")?;
        sst_writer.push_record(b"e", b"3")?;
        sst_writer.finish()?;

        let mut reader = SstReader::new(output.into_inner());

        reader.seek(b"");
        assert_eq!(reader.get(), Some((b"a".as_ref(), b"1".as_ref())));
        reader.seek(b"a");
        assert_eq!(reader.get(), Some((b"a".as_ref(), b"1".as_ref())));
        reader.seek(b"b");
        assert_eq!(reader.get(), Some((b"c".as_ref(), b"2".as_ref())));
        reader.seek(b"c");
        assert_eq!(reader.get(), Some((b"c".as_ref(), b"2".as_ref())));
        reader.seek(b"d");
        assert_eq!(reader.get(), Some((b"e".as_ref(), b"3".as_ref())));
        reader.seek(b"e");
        assert_eq!(reader.get(), Some((b"e".as_ref(), b"3".as_ref())));
        reader.seek(b"f");
        assert_eq!(reader.get(), None);
        Ok(())
    }

    #[test]
    fn test_sst_reader_advance() -> Result<(), Box<dyn Error>> {
        let mut output = Cursor::new(vec![]);
        let mut sst_writer = SstWriter::new(&mut output)?;
        sst_writer.push_record(b"a", b"1")?;
        sst_writer.push_record(b"c", b"2")?;
        sst_writer.push_record(b"e", b"3")?;
        sst_writer.finish()?;

        let mut reader = SstReader::new(output.into_inner());

        reader.seek(b"a");
        assert_eq!(reader.get(), Some((b"a".as_ref(), b"1".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), Some((b"c".as_ref(), b"2".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), Some((b"e".as_ref(), b"3".as_ref())));
        reader.advance();
        assert_eq!(reader.get(), None);
        Ok(())
    }

    #[test]
    fn test_sst_reader_with_btree() -> Result<(), Box<dyn Error>> {
        let mut output = Cursor::new(vec![]);
        let mut sst_writer = SstWriter::new(&mut output)?;
        // To get 2 btree levels we need > 16 * 64 records
        for i in 0..2000_i32 {
            sst_writer.push_record(&(i).to_be_bytes(), b"1")?;
        }

        sst_writer.finish()?;

        let mut reader = SstReader::new(output.into_inner());

        reader.seek(b"");
        assert_eq!(
            reader.get(),
            Some((0_i32.to_be_bytes().as_ref(), b"1".as_ref()))
        );

        reader.seek(500_i32.to_be_bytes().as_ref());
        assert_eq!(
            reader.get(),
            Some((500_i32.to_be_bytes().as_ref(), b"1".as_ref()))
        );

        reader.seek(1999_i32.to_be_bytes().as_ref());
        assert_eq!(
            reader.get(),
            Some((1999_i32.to_be_bytes().as_ref(), b"1".as_ref()))
        );

        reader.seek(2000_i32.to_be_bytes().as_ref());
        assert_eq!(reader.get(), None);
        Ok(())
    }
}
