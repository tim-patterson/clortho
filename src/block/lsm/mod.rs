use crate::block::file_store::FileStore;
/// Lsm tree, when thinking about how the whole kv store ties together at the top we have
/// snapshots and filestores,
/// A snapshot is a point in time copy of the state of the database, this is basically just a
/// collection of tables, each table being its own lsm tree.
/// A filestore is really the global access to the underlying files, with the memory mappings cached.
use crate::block::lsm::level::LsmLevelIter;
use crate::snapshot::TableSnapshot;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub mod level;

/// A Lsm Style iterator that works at the tree level of an lsm.
/// The idea here is that this iterator is dumb and doesn't know about merge records or delete
/// tombstones etc.
/// Upper layers will probably use the methods on this iter for range scans but for point look ups
/// they may instead decide to implement their own layer over the levels.
pub struct LsmIter<'a, F: FileStore> {
    pub levels: Vec<LsmLevelIter<'a, F>>,
    // A binary (min) heap containing the keys for all the current positions of the
    // child iters.
    // We'll have to play with lifetimes a bit to do this..
    heap: BinaryHeap<Next>,
}

/// Wrapper around the idx and next key of a level iter to allow us to create a
/// custom sort for the binary heap
#[derive(Eq, PartialEq, Debug)]
struct Next {
    level: usize,
    key: &'static [u8],
}

impl Ord for Next {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare by key first and then by level (ie higher (closer to 0) levels should
        // come first. Comparisons are swapped to trick the binaryheap from being a max
        // heap to a min heap.
        other
            .key
            .cmp(self.key)
            .then_with(|| other.level.cmp(&self.level))
    }
}

impl PartialOrd for Next {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, F: FileStore> LsmIter<'a, F> {
    /// Creates a new iter
    pub fn new(tree: &'a TableSnapshot, file_store: &'a F) -> Self {
        LsmIter {
            levels: tree
                .levels
                .iter()
                .map(|level| LsmLevelIter::new(level, file_store))
                .collect(),
            heap: BinaryHeap::with_capacity(tree.levels.len()),
        }
    }

    /// Seeks to the first record with a key equal to or greater than the given key
    pub fn seek(&mut self, key: &[u8]) -> Result<(), std::io::Error> {
        // Initial seek and populate heap
        self.heap.clear();
        for (idx, level) in self.levels.iter_mut().enumerate() {
            level.seek(key)?;
            if let Some((child_key, _)) = level.get() {
                // Fudge lifetimes
                let static_key = unsafe { std::mem::transmute::<&[u8], &[u8]>(child_key) };
                self.heap.push(Next {
                    level: idx,
                    key: static_key,
                });
            }
        }
        Ok(())
    }

    /// Advances to the next record
    pub fn advance(&mut self) -> Result<(), std::io::Error> {
        // Here we just pop off the top record and backfill it with another record from the same
        // iter
        if let Some(top) = self.heap.pop() {
            let level = &mut self.levels[top.level];
            level.advance()?;
            if let Some((child_key, _)) = level.get() {
                // Fudge lifetimes
                let static_key = unsafe { std::mem::transmute::<&[u8], &[u8]>(child_key) };
                self.heap.push(Next {
                    level: top.level,
                    key: static_key,
                });
            }
        }
        Ok(())
    }

    /// Returns the data at the current position
    pub fn get(&self) -> Option<(&[u8], &[u8])> {
        self.heap
            .peek()
            .and_then(|next| self.levels[next.level].get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::file_store::memory_file_store::MemoryFileStore;
    use crate::block::sst::sst_writer::SstWriter;
    use crate::snapshot::{LsmLevelSnapshot, NamedSst};
    use std::sync::Arc;

    #[test]
    fn test_lsm_iter() -> std::io::Result<()> {
        let file_store = MemoryFileStore::default();
        // write records for a couple levels
        let mut writer1 = SstWriter::new(file_store.open_for_write("01")?)?;
        writer1.push_record(b"a", b"1")?;
        writer1.push_record(b"b", b"1")?;
        writer1.push_record(b"e", b"1")?;
        writer1.push_record(b"g", b"1")?;
        let sst1 = writer1.finish()?;

        let mut writer2 = SstWriter::new(file_store.open_for_write("02")?)?;
        writer2.push_record(b"c", b"2")?;
        writer2.push_record(b"d", b"2")?;
        writer2.push_record(b"f", b"2")?;
        writer2.push_record(b"g", b"2")?;
        let sst2 = writer2.finish()?;

        let lsm_tree = TableSnapshot {
            levels: vec![
                Arc::new(LsmLevelSnapshot {
                    ssts: vec![Arc::new(NamedSst {
                        identifier: "01".to_string(),
                        info: sst1,
                    })],
                }),
                Arc::new(LsmLevelSnapshot {
                    ssts: vec![Arc::new(NamedSst {
                        identifier: "02".to_string(),
                        info: sst2,
                    })],
                }),
            ],
        };

        let mut lsm_iter = LsmIter::new(&lsm_tree, &file_store);

        // Test Seeks
        lsm_iter.seek(b"a")?;
        assert_eq!(lsm_iter.get(), Some((b"a".as_ref(), b"1".as_ref())));

        lsm_iter.seek(b"c")?;
        assert_eq!(lsm_iter.get(), Some((b"c".as_ref(), b"2".as_ref())));

        lsm_iter.seek(b"d")?;
        assert_eq!(lsm_iter.get(), Some((b"d".as_ref(), b"2".as_ref())));

        lsm_iter.seek(b"z")?;
        assert_eq!(lsm_iter.get(), None);

        // Test scan across levels
        lsm_iter.seek(b"b")?;
        assert_eq!(lsm_iter.get(), Some((b"b".as_ref(), b"1".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"c".as_ref(), b"2".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"d".as_ref(), b"2".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"e".as_ref(), b"1".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"f".as_ref(), b"2".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"g".as_ref(), b"1".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), Some((b"g".as_ref(), b"2".as_ref())));
        lsm_iter.advance()?;
        assert_eq!(lsm_iter.get(), None);
        Ok(())
    }
}
