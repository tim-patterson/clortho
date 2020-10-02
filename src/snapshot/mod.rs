/// A snapshot is a readonly copy of all the lsm sst metadata for the database.
/// Its a copy on write style structure with quite a few Arcs down the tree, this allows
/// us to share allot of state instead of doing full deep copies.
/// The levels of metadata are:
/// db:
///   table(lsm_tree):
///     lsm_level:
///        named_sst
use crate::block::sst::SstInfo;
use std::collections::HashMap;
use std::ops::Index;
use std::sync::Arc;

/// A point in time read view snapshot of the database
#[derive(Clone, Default)]
pub struct DbSnapshot {
    inner: Arc<DbSnapshotInner>,
}

#[derive(Default)]
struct DbSnapshotInner {
    tables: HashMap<String, Arc<TableSnapshot>>,
}

/// Abstraction for the lsm
pub struct TableSnapshot {
    pub levels: Vec<Arc<LsmLevelSnapshot>>,
}

/// A single level of the lsm
pub struct LsmLevelSnapshot {
    pub ssts: Vec<Arc<NamedSst>>,
}

/// Sst info coupled with filename
pub struct NamedSst {
    pub identifier: String,
    pub info: SstInfo,
}

impl Index<&str> for DbSnapshot {
    type Output = TableSnapshot;

    fn index(&self, index: &str) -> &Self::Output {
        &self.inner.tables[index]
    }
}
