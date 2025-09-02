// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

pub trait StorageEngine {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()>;
    fn get(&self, key: &str) -> Arc<[u8]>;
    fn delete(&mut self, key: &str) -> std::io::Result<()>;
    // when restoring from file system we need a way to tell nkv
    // what are the keys available, so it could get a structure
    fn keys(&self) -> Vec<String>;
}
