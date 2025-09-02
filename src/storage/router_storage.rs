// SPDX-License-Identifier: Apache-2.0

use super::{file_storage::FileStorage, memory_storage::MemoryStorage, traits::StorageEngine};

#[derive(Debug)]
pub struct RouterStorage {
    mem: MemoryStorage,
    file: FileStorage,
}

/// `RouterStorage` is a hybrid storage backend that delegates keys to different
/// storage implementations depending on their prefix.
///
/// In some cases, you may want certain values to live only in memory (and thus
/// disappear after a reboot), while others should be stored persistently on disk.
/// This is exactly where `RouterStorage` shines.
///
/// - Keys under the `runtime.*` tree are stored **in-memory**.
/// - All other keys are stored **in files**.
///
/// The design is intentionally kept simple. In a more flexible approach, one might
/// define a meta-trait that parameterizes the memory and file storage backends, as
/// well as the runtime prefix constant. That would allow choosing between
/// backends via a builder or configuration mechanism.  
///
/// For now, we omit this complexity in favor of simplicity, while still delivering
/// the same functionality. If you need this extended flexibility, please raise a
/// ticket.
impl RouterStorage {
    const RUNTIME_ROUTE: &'static str = "runtime";

    pub fn new(mem: MemoryStorage, file: FileStorage) -> std::io::Result<Self> {
        Ok(Self { mem, file })
    }
}

impl StorageEngine for RouterStorage {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()> {
        if key.starts_with(Self::RUNTIME_ROUTE) {
            self.mem.put(key, data)
        } else {
            self.file.put(key, data)
        }
    }

    fn delete(&mut self, key: &str) -> std::io::Result<()> {
        if key.starts_with(Self::RUNTIME_ROUTE) {
            self.mem.delete(key)
        } else {
            self.file.delete(key)
        }
    }

    fn get(&self, key: &str) -> std::sync::Arc<[u8]> {
        if key.starts_with(Self::RUNTIME_ROUTE) {
            self.mem.get(key)
        } else {
            self.file.get(key)
        }
    }

    fn keys(&self) -> Vec<String> {
        let mut result = self.mem.keys();
        result.extend(self.file.keys());
        result
    }
}
