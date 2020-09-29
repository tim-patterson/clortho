use crate::file_store::FileStore;
use crate::lsm::NamedSst;
use crate::sst::sst_reader::SstReader;
use std::cmp::Ordering;

/// A single level of the lsm
pub struct LsmLevel {
    pub ssts: Vec<NamedSst>,
}

/// A lsm style iterator that works across a single lsm level
pub struct LsmLevelIter<'a, F: FileStore> {
    level: &'a LsmLevel,
    file_store: &'a F,
    current_sst: Option<(SstReader<F::R>, usize)>,
}

impl<'a, F: FileStore> LsmLevelIter<'a, F> {
    /// Creates a new iter
    pub fn new(level: &'a LsmLevel, file_store: &'a F) -> Self {
        LsmLevelIter {
            level,
            file_store,
            current_sst: None,
        }
    }

    /// Seeks to the first record with a key equal to or greater than the given key
    pub fn seek(&mut self, key: &[u8]) -> Result<(), std::io::Error> {
        let sst_idx = self.level.ssts.binary_search_by(|sst| {
            if sst.info.max_record.as_ref() < key {
                Ordering::Less
            } else if sst.info.min_record.as_ref() > key {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });
        // For a seek we need to upgrade the errs (seek between the files) to Ok's
        // except where the seek is off the upper end...
        let sst_offet = sst_idx.unwrap_or_else(|e| e);
        if sst_offet < self.level.ssts.len() {
            let sst = &self.level.ssts[sst_offet];
            let raw = self.file_store.open_for_read(&sst.identifier)?;
            let mut sst_reader = SstReader::new(raw);
            sst_reader.seek(key);
            self.current_sst = Some((sst_reader, sst_offet));
        } else {
            self.current_sst = None;
        }
        Ok(())
    }

    /// Advances to the next record
    pub fn advance(&mut self) -> Result<(), std::io::Error> {
        if let Some((reader, idx)) = &mut self.current_sst {
            reader.advance();
            // If we've run off the end we'll attempt to load the next sst.
            if reader.get().is_none() {
                let next = *idx + 1;
                if next < self.level.ssts.len() {
                    let sst = &self.level.ssts[next];
                    let raw = self.file_store.open_for_read(&sst.identifier)?;
                    let mut sst_reader = SstReader::new(raw);
                    sst_reader.seek(b"");
                    self.current_sst = Some((sst_reader, next));
                }
            }
        }
        Ok(())
    }

    /// Returns the data at the current position
    pub fn get(&self) -> Option<(&[u8], &[u8])> {
        self.current_sst
            .as_ref()
            .and_then(|(reader, _)| reader.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::memory_file_store::MemoryFileStore;
    use crate::sst::sst_writer::SstWriter;

    #[test]
    fn test_lsm_level_iter() -> std::io::Result<()> {
        let file_store = MemoryFileStore::default();
        // write records across 2 ssts

        let mut writer1 = SstWriter::new(file_store.open_for_write("01")?)?;
        writer1.push_record(b"a", b"1")?;
        writer1.push_record(b"b", b"2")?;
        writer1.push_record(b"c", b"3")?;
        let sst1 = writer1.finish()?;

        let mut writer2 = SstWriter::new(file_store.open_for_write("02")?)?;
        writer2.push_record(b"d", b"4")?;
        writer2.push_record(b"e", b"5")?;
        writer2.push_record(b"f", b"6")?;
        let sst2 = writer2.finish()?;

        let lsm_level = LsmLevel {
            ssts: vec![
                NamedSst {
                    identifier: "01".to_string(),
                    info: sst1,
                },
                NamedSst {
                    identifier: "02".to_string(),
                    info: sst2,
                },
            ],
        };

        let mut lsm_iter = LsmLevelIter::new(&lsm_level, &file_store);

        // Test Seeks
        lsm_iter.seek(b"a")?;
        assert_eq!(lsm_iter.get(), Some((b"a".as_ref(), b"1".as_ref())));

        lsm_iter.seek(b"c")?;
        assert_eq!(lsm_iter.get(), Some((b"c".as_ref(), b"3".as_ref())));

        lsm_iter.seek(b"d")?;
        assert_eq!(lsm_iter.get(), Some((b"d".as_ref(), b"4".as_ref())));

        lsm_iter.seek(b"z")?;
        assert_eq!(lsm_iter.get(), None);

        // Test scan across ssts
        lsm_iter.seek(b"c")?;
        assert_eq!(lsm_iter.get(), Some((b"c".as_ref(), b"3".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"d".as_ref(), b"4".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"e".as_ref(), b"5".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"f".as_ref(), b"6".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), None);
        Ok(())
    }

    /// Test for where we seek to before the start of the sst's.
    #[test]
    fn test_lsm_level_iter_pre() -> std::io::Result<()> {
        let file_store = MemoryFileStore::default();
        // write records across 2 ssts

        let mut writer1 = SstWriter::new(file_store.open_for_write("01")?)?;
        writer1.push_record(b"a", b"1")?;
        writer1.push_record(b"b", b"2")?;
        writer1.push_record(b"c", b"3")?;
        let sst1 = writer1.finish()?;

        let lsm_level = LsmLevel {
            ssts: vec![NamedSst {
                identifier: "01".to_string(),
                info: sst1,
            }],
        };

        let mut lsm_iter = LsmLevelIter::new(&lsm_level, &file_store);

        // Test Seeks
        lsm_iter.seek(b"")?;
        assert_eq!(lsm_iter.get(), Some((b"a".as_ref(), b"1".as_ref())));
        Ok(())
    }
}
