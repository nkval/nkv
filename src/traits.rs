// SPDX-License-Identifier: Apache-2.0

use crate::notifier::Notifier;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait StoragePolicy: PartialEq {
    fn new(data: Box<[u8]>, filepath: PathBuf) -> std::io::Result<Self>
    where
        Self: Sized;

    fn from_checkpoint(filepath: &Path) -> std::io::Result<Self>
    where
        Self: Sized;
    fn update(&mut self, new_data: Box<[u8]>) -> std::io::Result<()>;
    fn delete_checkpoint(&self) -> std::io::Result<()>;
    fn data(&self) -> Arc<[u8]>;
}

#[derive(Debug)]
pub struct Value<V: StoragePolicy> {
    pub pv: V,
    pub notifier: Arc<Mutex<Notifier>>,
}
