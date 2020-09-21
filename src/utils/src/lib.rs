use std::time::SystemTime;

pub mod varint;

/// A Timestamp struct, just a wrapper around unix epoch.
#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Default, Debug)]
pub struct Timestamp {
    pub ms: u64,
}

impl Timestamp {
    pub fn now() -> Self {
        Timestamp {
            ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}
