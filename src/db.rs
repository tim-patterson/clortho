use crate::snapshot::DbSnapshot;
use std::sync::RwLock;

/// Top level entry for interacting with the database
pub struct Db {
    current_snapshot: RwLock<DbSnapshot>,
}

impl Db {
    /// Creates a new database ( in memory )
    pub fn new_in_mem() -> Db {
        Db {
            current_snapshot: RwLock::new(DbSnapshot::default()),
        }
    }

    /// Returns a point in time snapshot for reads..
    pub fn read(&self) -> DbSnapshot {
        self.current_snapshot.read().unwrap().clone()
    }

    /// A write "transaction", writes wont be committed to the lsm until this function returns
    pub fn write<F>(&self, writer_function: F) -> std::io::Result<()>
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        writer_function()
    }
}
