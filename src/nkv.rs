// SPDX-License-Identifier: Apache-2.0

// NotifyKeyValue structure is a persistent key-value
// storage with ability to notify clients about changes
// made in a value. When created via new() it will try to
// load values from folder. Underlying structure is a HashMap
// and designed to be access synchronously.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::errors::NotifyKeyValueError;
use crate::request_msg::Message;
use crate::storage::traits::StorageEngine;
use crate::trie::{Trie, TrieNode};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error};

#[derive(Debug)]
pub enum NotificationError {
    AlreadySubscribed(String),
    SendError(String),
}

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationError::AlreadySubscribed(s) => write!(f, "Already subscribed {}", s),
            NotificationError::SendError(s) => write!(f, "Failed to send {}", s),
        }
    }
}
impl std::error::Error for NotificationError {}

impl<T> From<mpsc::error::SendError<T>> for NotificationError {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        NotificationError::SendError("Failed to send message".to_string())
    }
}

#[derive(Debug)]
pub struct Notification {
    // Key is client UUID which requested notification
    subscriptions: HashMap<String, mpsc::UnboundedSender<Message>>,
}

impl Notification {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    pub fn subscribe(
        &mut self,
        uuid: String,
    ) -> Result<mpsc::UnboundedReceiver<Message>, NotificationError> {
        if self.subscriptions.contains_key(&uuid) {
            return Err(NotificationError::AlreadySubscribed(uuid));
        }
        let (tx, rx) = mpsc::unbounded_channel::<Message>();
        self.subscriptions.insert(uuid, tx.clone());
        tx.send(Message::Hello)?;
        Ok(rx)
    }

    pub fn unsubscribe(&mut self, key: String, uuid: &str) -> Result<(), NotificationError> {
        if let Some(tx) = self.subscriptions.remove(uuid) {
            tx.send(Message::Close {
                key: key.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn unsubscribe_all(&mut self, key: &str) -> Result<(), NotificationError> {
        for (_, tx) in self.subscriptions.drain() {
            tx.send(Message::Close {
                key: key.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn send_hello(&self) -> Result<(), NotificationError> {
        for (_, tx) in &self.subscriptions {
            tx.send(Message::Hello)?;
        }
        Ok(())
    }

    pub fn send_update(&self, key: String, value: Box<[u8]>) -> Result<(), NotificationError> {
        for (_, tx) in &self.subscriptions {
            tx.send(Message::Update {
                key: key.clone(),
                value: value.clone(),
            })?;
        }
        Ok(())
    }

    pub fn send_close(&self, key: String) -> Result<(), NotificationError> {
        for (_, tx) in &self.subscriptions {
            tx.send(Message::Close { key: key.clone() })?;
        }
        Ok(())
    }
}

type N = Arc<Mutex<Notification>>;

/// NkvCore - The Heart of the Hierarchical Key-Value System
///
/// ## Overview
/// NkvCore is the core component of the entire system. In essence, it provides a
/// hierarchical key-value storage with notifications, built on top of a pluggable
/// storage trait architecture.
///
/// ## Architecture Flexibility
/// The storage trait design means you can implement any storage strategy you want:
/// - File-based storage
/// - Database backends
/// - In-memory storage
/// - Custom implementations
///
/// You can plug these into either:
/// - Your own binary directly
/// - The NkvCore process for inter-thread communication within a single binary
///
/// ## Storage Abstraction
/// The storage layer is intentionally simple and doesn't know about hierarchy:
/// - Store a value for a given unique key
/// - Retrieve a value by key
/// - Delete a value by key
///
/// ## Hierarchical Structure
/// The Trie is the underlying data structure that handles hierarchical relationships.
/// For more details, see: `../docs/KEYSPACES.md`
///
/// ## Dual-Tree Architecture
/// Notifications and objects are managed separately for performance reasons:
///
/// ### Why Separate Trees?
/// - **Notifications**: Managed by `notifiers` - references to notification handlers
/// - **Objects**: Managed by `objects` - references to storage keys for the StorageEngine
///
/// ### Performance Benefits
/// If notifications and files were stored together:
/// - Combined traversal: `O(M + N)` where M = notifications, N = files
///
/// With separate trees:
/// - Sequential traversal: `O(M) + O(N)`
/// - More efficient when you only need one type of data
///
/// ### Usage Patterns
/// - **Objects tree**: Traversed to get storage keys for the StorageEngine to handle files
/// - **Notifiers tree**: Traversed when updating subscribers about changes
///
/// ## Core API
/// NKV provides five primary operations:
/// - `GET` - Retrieve values
/// - `PUT` - Store values
/// - `DELETE` - Remove values
/// - `SUBSCRIBE` - Register for notifications
/// - `UNSUBSCRIBE` - Unregister from notifications
///
/// For detailed protocol information, see: `../docs/CLIENT_SERVER_PROTOCOL.md`
pub struct NkvCore<P: StorageEngine> {
    notifiers: Trie<N>,
    objects: Trie<String>,
    storage: P,
}

impl<P: StorageEngine> NkvCore<P> {
    pub fn new(storage: P) -> std::io::Result<Self> {
        let mut objects = Trie::new();
        for k in storage.keys() {
            if let Some(e) = objects.insert(&k, k.clone()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Trie insert {} failed: {}", k, e),
                ));
            }
        }
        let res = Self {
            notifiers: Trie::new(),
            objects,
            storage,
        };
        Ok(res)
    }

    pub async fn put(&mut self, key: &str, value: Box<[u8]>) -> Result<(), NotifyKeyValueError> {
        if Trie::<()>::has_wildcard(key) {
            return Err(NotifyKeyValueError::WrongKey(
                "put does not support wildcards".to_string(),
            ));
        }
        if !Self::is_key_correct(key) {
            return Err(NotifyKeyValueError::WrongKey(format!(
                "key {} is invalid",
                key
            )));
        }
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value),
                        None => return Box::pin(async {}),
                    };

                    let vector_clone = Arc::clone(&vc);

                    Box::pin(async move {
                        let mut vector_lock = vector_clone.lock().await;
                        vector_lock.push(notifier_arc);
                    })
                },
            )
        });

        self.objects.insert(key, key.into());
        self.storage.put(key, value.clone()).map_err(|err| {
            error!("failed to store value: {}", err);
            err
        })?;
        debug!("we put value for {}", key);

        let notifiers = self.notifiers.get_mut(key, capture_and_push).await;

        match notifiers.len() {
            0 => {} // ignore
            1 => {
                match notifiers[0]
                    .lock()
                    .await
                    .send_update(key.to_string(), value.clone())
                {
                    Err(err) => error!("failed to send update notification for {}: {}", key, err),
                    _ => {}
                }
            }
            _ => {
                // TODO: you can't put value with a wildcard,
                // so it should not return more than one value
            }
        };

        let vector_lock = vector.lock().await;
        for notifier_arc in vector_lock.iter() {
            // TODO: write an error log connecting to specific notifier?
            notifier_arc
                .lock()
                .await
                .send_update(key.to_string(), value.clone())?;
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> HashMap<String, Arc<[u8]>> {
        let map: HashMap<String, Arc<[u8]>> = self
            .objects
            .get(key)
            .iter()
            .map(|s| (s.to_string(), self.storage.get(s)))
            .collect();
        return map;
    }

    pub async fn delete(&mut self, key: &str) -> Result<(), NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value),
                        None => return Box::pin(async {}),
                    };

                    let vector_clone = Arc::clone(&vc);

                    Box::pin(async move {
                        let mut vector_lock = vector_clone.lock().await;
                        vector_lock.push(notifier_arc);
                    })
                },
            )
        });

        // delete all
        let notifiers = self.notifiers.get_mut(key, capture_and_push).await;

        // if we have wildcard, we want to delete all children as well
        for n in &notifiers {
            n.lock().await.unsubscribe_all(key)?
        }

        // notify all parents that element(s) is(are) being deleted
        {
            let vec_lock = vector.lock().await;
            for n_arc in vec_lock.iter() {
                n_arc.lock().await.send_close(key.to_string())?;
            }
        }

        let objs = self.objects.get(key);

        for obj in objs {
            self.storage.delete(obj).map_err(|err| {
                error!("failed to delete {} from storage: {}", key, err);
                err
            })?;
        }

        self.notifiers.remove(key);
        self.objects.remove(key);

        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<mpsc::UnboundedReceiver<Message>, NotifyKeyValueError> {
        // XXX: should there be a wildcard restriction?
        let notifiers = self.notifiers.get_mut(key, None).await;

        match notifiers.len() {
            0 => {
                // Client can subscribe to a non-existent value
                let n = Arc::new(Mutex::new(Notification::new()));
                let tx = n.lock().await.subscribe(uuid).map_err(|err| {
                    error!("failed to subscribe: {}", err); // TODO: add uuid here?
                    err
                })?;
                self.notifiers.insert(key, n);
                return Ok(tx);
            }
            1 => {
                return Ok(notifiers[0].lock().await.subscribe(uuid)?);
            }
            // TODO: add new error
            _ => return Err(NotifyKeyValueError::NotFound),
        }
    }

    pub async fn unsubscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<(), NotifyKeyValueError> {
        // XXX: should there be a wildcard restriction?
        let notifiers = self.notifiers.get_mut(key, None).await;

        match notifiers.len() {
            1 => {
                notifiers[0]
                    .lock()
                    .await
                    .unsubscribe(key.to_string(), &uuid)
                    .map_err(|err| {
                        error!("failed to unsubscribe for key {}: {}", key, err);
                        err
                    })?;
                Ok(())
            }
            // TODO: add new error when its more than 1, for 0 should
            // return NotFound
            _ => Err(NotifyKeyValueError::NotFound),
        }
    }
    pub async fn trace(&mut self, key: &str) -> Result<Vec<String>, NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value),
                        None => return Box::pin(async {}),
                    };

                    let vector_clone = Arc::clone(&vc);

                    Box::pin(async move {
                        let mut vector_lock = vector_clone.lock().await;
                        vector_lock.push(notifier_arc);
                    })
                },
            )
        });

        let mut cloned_vec: Vec<String> = Vec::new();

        // include
        for n in self.notifiers.get_mut(key, capture_and_push).await {
            for key in n.lock().await.subscriptions.keys().cloned() {
                cloned_vec.push(key);
            }
        }

        let vector_lock = vector.lock().await;
        for notifier_arc in vector_lock.iter() {
            for key in notifier_arc.lock().await.subscriptions.keys().cloned() {
                cloned_vec.push(key);
            }
        }
        return Ok(cloned_vec);
    }

    fn is_key_correct(key: &str) -> bool {
        // to speed things up, we match key to a regexp, downside is that
        // error is not self explainable
        use regex::Regex;

        // ^(
        //    \* --> wildcard alone
        //    |                  --> OR
        //    .+                 --> any character (except newline)
        //    (\..+)* --> optional .key segments with any characters
        //    (\.\*)?            --> optional .*
        // )$
        let re = Regex::new(r"^(\*|.+(\..+)*(\.\*)?)$").unwrap();
        re.is_match(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::file_storage::FileStorage;

    use super::*;
    use anyhow::Result;
    use tempfile::TempDir;
    use tokio;

    #[tokio::test]
    async fn test_put_and_get() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await?;

        let result = nkv.get("key1");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key1".to_string(), Arc::from(data));
        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let nkv = NkvCore::new(storage)?;

        let result = nkv.get("nonexistent_key");
        assert_eq!(result, HashMap::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await?;

        nkv.delete("key1").await?;
        let result = nkv.get("key1");
        assert_eq!(result, HashMap::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await?;

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("key1", new_data.clone()).await?;

        let result = nkv.get("key1");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key1".to_string(), Arc::from(new_data));
        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_load_nkv() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();
        let data1: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        let data2: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        let data3: Box<[u8]> = Box::new([10, 11, 12, 13, 14]);

        {
            let storage = FileStorage::new(path.clone())?;
            let mut nkv = NkvCore::new(storage)?;
            nkv.put("key1", data1.clone()).await?;
            nkv.put("key2", data2.clone()).await?;
            nkv.put("key3", data3.clone()).await?;
        }

        let storage = FileStorage::new(path)?;
        let nkv = NkvCore::new(storage)?;
        let result = nkv.get("key1");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key1".to_string(), Arc::from(data1));
        assert_eq!(result, expected);

        let result = nkv.get("key2");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key2".to_string(), Arc::from(data2));
        assert_eq!(result, expected);

        let result = nkv.get("key3");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key3".to_string(), Arc::from(data3));
        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await?;

        let mut rx = nkv.subscribe("key1", "uuid1".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("key1", new_data.clone()).await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Update {
                    key: "key1".to_string(),
                    value: new_data.clone()
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        let result = nkv.get("key1");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("key1".to_string(), Arc::from(new_data));
        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_keyspace() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("ks1.ks2.ks3.k", data).await?;

        let mut rx = nkv.subscribe("ks1", "uuid1".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("ks1.ks2.ks4", new_data.clone()).await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Update {
                    key: "ks1.ks2.ks4".to_string(),
                    value: new_data.clone()
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        let result = nkv.get("ks1.ks2.ks4");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("ks1.ks2.ks4".to_string(), Arc::from(new_data));
        assert_eq!(result, expected);

        nkv.delete("ks1.ks2.ks4").await.unwrap();

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "ks1.ks2.ks4".to_string(),
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unsubscribe() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await?;

        let mut rx = nkv.subscribe("key1", "uuid1".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }
        nkv.unsubscribe("key1", "uuid1".to_string()).await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "key1".to_string()
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_notification() -> Result<()> {
        let mut n = Notification::new();

        let mut rx = n.subscribe("uuid1".to_string())?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }

        n.send_hello()?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        n.send_update("key1".to_string(), data.clone())?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Update {
                    key: "key1".to_string(),
                    value: data.clone()
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        n.send_close("key1".to_string())?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "key1".to_string(),
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_keyspace() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data1: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        let data2: Box<[u8]> = Box::new([6, 7, 8, 9, 10]);
        let data3: Box<[u8]> = Box::new([11, 12, 13, 14, 15]);
        // ks1 -> ks2 -> ks3 -> ks4 -> k
        //     |         \             -> data1
        //     -> data2  |
        //                -> data3
        nkv.put("ks1.ks2.ks3.ks4.k", data1.clone()).await?;
        nkv.put("ks1", data2.clone()).await?;
        nkv.put("ks1.ks2", data3.clone()).await?;

        let result = nkv.get("ks1.ks2.ks3.ks4.k");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("ks1.ks2.ks3.ks4.k".to_string(), Arc::from(data1.clone()));
        assert_eq!(result, expected);

        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("ks1.ks2.ks3.ks4.k".to_string(), Arc::from(data1.clone()));
        expected.insert("ks1".to_string(), Arc::from(data2.clone()));
        expected.insert("ks1.ks2".to_string(), Arc::from(data3.clone()));

        let result = nkv.get("ks1.*");
        assert_eq!(result, expected);

        let result = nkv.get("*");
        assert_eq!(result, expected);

        let result = nkv.get("ks1.ks2.*");
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("ks1.ks2.ks3.ks4.k".to_string(), Arc::from(data1.clone()));
        expected.insert("ks1.ks2".to_string(), Arc::from(data3.clone()));
        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_trace() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("ks1.ks2.ks3.k", data).await?;

        let result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert_eq!(result.len(), 0);

        let result = nkv.trace("non.existent.key").await.unwrap();
        assert_eq!(result.len(), 0);

        let mut rx = nkv.subscribe("ks1", "uuid1".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }

        let result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert_eq!(result, vec!["uuid1".to_string()]);

        let mut rx = nkv.subscribe("ks1.ks2", "uuid2".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }
        let mut result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert_eq!(
            result.sort(),
            vec!["uuid1".to_string(), "uuid2".to_string()].sort()
        );

        let mut rx = nkv.subscribe("ks1.ks2.ks3", "uuid3".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }
        let mut result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert_eq!(
            result.sort(),
            vec![
                "uuid1".to_string(),
                "uuid2".to_string(),
                "uuid3".to_string()
            ]
            .sort()
        );

        let mut rx = nkv.subscribe("ks1.ks2.ks3.k", "uuid4".to_string()).await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should recieve msg");
        }
        let mut result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert_eq!(
            result.sort(),
            vec![
                "uuid4".to_string(),
                "uuid1".to_string(),
                "uuid2".to_string(),
                "uuid3".to_string()
            ]
            .sort()
        );

        nkv.unsubscribe("ks1.ks2.ks3.k", "uuid4".to_string())
            .await?;
        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "ks1.ks2.ks3.k".to_string()
                }
            )
        } else {
            panic!("Should recieve msg");
        }

        let result = nkv.trace("ks1.ks2.ks3.k").await.unwrap();
        assert!(!result.contains(&"uuid4".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_wildcard_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data1: Box<[u8]> = Box::new([1, 2, 3]);
        let data2: Box<[u8]> = Box::new([4, 5, 6]);
        let data3: Box<[u8]> = Box::new([7, 8, 9]);
        let data4: Box<[u8]> = Box::new([10, 11, 12]);

        nkv.put("ks1.child1", data1.clone()).await?;
        nkv.put("ks1.child2", data2.clone()).await?;
        nkv.put("ks1.deep.child", data3.clone()).await?;
        nkv.put("ks2.child", data4.clone()).await?;

        // Delete all children of ks1
        nkv.delete("ks1.*").await?;

        // ks1 children should be gone
        assert_eq!(nkv.get("ks1.child1"), HashMap::new());
        assert_eq!(nkv.get("ks1.child2"), HashMap::new());
        assert_eq!(nkv.get("ks1.deep.child"), HashMap::new());

        // ks2.child should still exist
        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert("ks2.child".to_string(), Arc::from(data4));
        assert_eq!(nkv.get("ks2.child"), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_wildcard_delete_all() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data1: Box<[u8]> = Box::new([1, 2, 3]);
        let data2: Box<[u8]> = Box::new([4, 5, 6]);

        nkv.put("key1", data1.clone()).await?;
        nkv.put("ks1.child", data2.clone()).await?;

        // Delete everything
        nkv.delete("*").await?;

        assert_eq!(nkv.get("key1"), HashMap::new());
        assert_eq!(nkv.get("ks1.child"), HashMap::new());
        assert_eq!(nkv.get("*"), HashMap::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_key_with_only_dots() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3]);

        // Test keys that are just dots
        assert!(nkv.put(".", data.clone()).await.is_err());
        assert!(nkv.put("..", data.clone()).await.is_err());
        assert!(nkv.put("...", data.clone()).await.is_err());

        let result = nkv.get(".");
        assert!(result.is_empty());

        let result = nkv.get("..");
        assert!(result.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_put_empty_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3]);

        // Test keys that are just dots
        assert!(nkv.put("", data.clone()).await.is_err());

        let result = nkv.get("*");
        assert!(result.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_to_nonexistent_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        // Subscribe to key that doesn't exist yet
        let mut rx = nkv.subscribe("future.key", "uuid1".to_string()).await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, Message::Hello)
        } else {
            panic!("Should receive hello msg");
        }

        // Now create the key
        let data: Box<[u8]> = Box::new([1, 2, 3]);
        nkv.put("future.key", data.clone()).await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Update {
                    key: "future.key".to_string(),
                    value: data.clone()
                }
            )
        } else {
            panic!("Should receive update msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_subscribers_same_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3]);
        nkv.put("shared.key", data.clone()).await?;

        // Multiple subscribers to same key
        let mut rx1 = nkv.subscribe("shared.key", "uuid1".to_string()).await?;
        let mut rx2 = nkv.subscribe("shared.key", "uuid2".to_string()).await?;
        let mut rx3 = nkv.subscribe("shared.key", "uuid3".to_string()).await?;

        // Consume hello messages
        rx1.recv().await;
        rx2.recv().await;
        rx3.recv().await;

        // Update the key
        let new_data: Box<[u8]> = Box::new([4, 5, 6]);
        nkv.put("shared.key", new_data.clone()).await?;

        // All should receive update
        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            if let Some(msg) = rx.recv().await {
                assert_eq!(
                    msg,
                    Message::Update {
                        key: "shared.key".to_string(),
                        value: new_data.clone()
                    }
                )
            } else {
                panic!("Should receive update msg");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_subscribe_unsubscribe_same_uuid_different_keys() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data1: Box<[u8]> = Box::new([1, 2, 3]);
        let data2: Box<[u8]> = Box::new([4, 5, 6]);
        nkv.put("key1", data1.clone()).await?;
        nkv.put("key2", data2.clone()).await?;

        // Same UUID subscribing to different keys
        let mut rx1 = nkv.subscribe("key1", "same_uuid".to_string()).await?;
        let mut rx2 = nkv.subscribe("key2", "same_uuid".to_string()).await?;

        // Consume hello messages
        rx1.recv().await;
        rx2.recv().await;

        // Unsubscribe from key1 only
        nkv.unsubscribe("key1", "same_uuid".to_string()).await?;

        if let Some(msg) = rx1.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "key1".to_string()
                }
            )
        } else {
            panic!("Should receive close msg");
        }

        // Update key2 - should still receive notification
        let new_data: Box<[u8]> = Box::new([7, 8, 9]);
        nkv.put("key2", new_data.clone()).await?;

        if let Some(msg) = rx2.recv().await {
            assert_eq!(
                msg,
                Message::Update {
                    key: "key2".to_string(),
                    value: new_data.clone()
                }
            )
        } else {
            panic!("Should receive update msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_notifies_subscribers() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data: Box<[u8]> = Box::new([1, 2, 3]);
        nkv.put("temp.key", data.clone()).await?;

        // Subscribe to parent keyspace
        let mut rx = nkv.subscribe("temp", "uuid1".to_string()).await?;
        rx.recv().await; // consume hello

        // Delete the key
        nkv.delete("temp.key").await?;

        if let Some(msg) = rx.recv().await {
            assert_eq!(
                msg,
                Message::Close {
                    key: "temp.key".to_string()
                }
            )
        } else {
            panic!("Should receive close msg");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_wildcard_delete_notifies_subscribers() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        let data1: Box<[u8]> = Box::new([1, 2, 3]);
        let data2: Box<[u8]> = Box::new([4, 5, 6]);
        nkv.put("batch.key1", data1.clone()).await?;
        nkv.put("batch.key2", data2.clone()).await?;

        let mut rx = nkv.subscribe("batch", "uuid1".to_string()).await?;
        rx.recv().await; // consume hello

        // Wildcard delete
        nkv.delete("batch.*").await?;

        if let Some(msg) = rx.recv().await {
            if let Message::Close { key } = msg {
                assert_eq!(key, "batch.*");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_1mb_data() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        // Test with large data (1MB)
        let large_data: Box<[u8]> = vec![42u8; 1024 * 1024].into_boxed_slice();
        nkv.put("large.data", large_data.clone()).await?;

        let result = nkv.get("large.data");
        assert_eq!(result.len(), 1);
        assert_eq!(result["large.data"].len(), 1024 * 1024);

        // Test subscription with large data
        let mut rx = nkv.subscribe("large.data", "uuid1".to_string()).await?;
        rx.recv().await; // consume hello

        let new_large_data: Box<[u8]> = vec![84u8; 1024 * 1024].into_boxed_slice();
        nkv.put("large.data", new_large_data.clone()).await?;

        if let Some(msg) = rx.recv().await {
            if let Message::Update { key, value } = msg {
                assert_eq!(key, "large.data");
                assert_eq!(value.len(), 1024 * 1024);
            } else {
                panic!("Should receive update msg");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_unsubscribe_nonexistent() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = FileStorage::new(temp_dir.path().to_path_buf())?;
        let mut nkv = NkvCore::new(storage)?;

        assert!(nkv
            .unsubscribe("nonexistent.key", "uuid1".to_string())
            .await
            .is_err());

        Ok(())
    }
}
