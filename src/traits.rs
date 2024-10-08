// SPDX-License-Identifier: Apache-2.0

use crate::errors::NotifierError;
use async_trait::async_trait;
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

#[async_trait]
pub trait Notifier {
    type W;

    fn new() -> Self
    where
        Self: Sized;

    async fn subscribe(&self, uuid: String, stream: Self::W) -> Result<(), NotifierError>;
    async fn unsubscribe(&self, key: String, uuid: String) -> Result<(), NotifierError>;
    async fn unsubscribe_all(&self, key: &str) -> Result<(), NotifierError>;

    async fn send_hello(&mut self);
    async fn send_update(&mut self, key: String, new_value: Box<[u8]>);
    async fn send_close(&mut self, key: String);
}

#[derive(Debug)]
pub struct Value<V: StoragePolicy, N: Notifier> {
    pub pv: V,
    pub notifier: Arc<Mutex<N>>,
}
