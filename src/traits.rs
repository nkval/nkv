// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

pub trait StorageEngine {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()>;
    fn get(&self, key: &str) -> Vec<Arc<[u8]>>;
    fn delete(&self, key: &str) -> std::io::Result<()>;
}
