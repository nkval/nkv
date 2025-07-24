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
use crate::traits::StorageEngine;
use crate::trie::{Trie, TrieNode};
use tokio::sync::{mpsc, Mutex};
use tracing::error;

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

pub struct NkvCore<P: StorageEngine> {
    notifiers: Trie<N>,
    storage: P,
}

impl<P: StorageEngine> NkvCore<P> {
    pub fn new(storage: P) -> std::io::Result<Self> {
        let res = Self {
            notifiers: Trie::new(),
            storage,
        };

        Ok(res)
    }

    pub async fn put(&mut self, key: &str, value: Box<[u8]>) -> Result<(), NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
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

        self.storage.put(key, value.clone()).map_err(|err| {
            error!("failed to store value: {}", err);
            err
        })?;

        if let Some(notifier) = self.notifiers.get_mut(key, capture_and_push).await {
            match notifier
                .lock()
                .await
                .send_update(key.to_string(), value.clone())
            {
                Err(err) => error!("failed to send update notification for {}: {}", key, err),
                _ => {}
            }
        }

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
        self.storage.get(key)
    }

    pub async fn delete(&mut self, key: &str) -> Result<(), NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
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

        if let Some(val) = self.notifiers.get_mut(key, capture_and_push).await {
            {
                let vec_lock = vector.lock().await;
                for n_arc in vec_lock.iter() {
                    n_arc.lock().await.send_close(key.to_string())?;
                }
            }
            val.lock().await.unsubscribe_all(key)?;
        }
        self.storage.delete(key).map_err(|err| {
            error!("failed to delete {} from storage: {}", key, err);
            err
        })?;
        self.notifiers.remove(key);
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<mpsc::UnboundedReceiver<Message>, NotifyKeyValueError> {
        if let Some(val) = self.notifiers.get_mut(key, None).await {
            return Ok(val.lock().await.subscribe(uuid)?);
        } else {
            // Client can subscribe to a non-existent value
            let n = Arc::new(Mutex::new(Notification::new()));
            let tx = n.lock().await.subscribe(uuid).map_err(|err| {
                error!("failed to subscribe: {}", err); // TODO: add uuid here?
                err
            })?;
            self.notifiers.insert(key, n);
            return Ok(tx);
        }
    }

    pub async fn unsubscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<(), NotifyKeyValueError> {
        if let Some(val) = self.notifiers.get_mut(key, None).await {
            val.lock()
                .await
                .unsubscribe(key.to_string(), &uuid)
                .map_err(|err| {
                    error!("failed to unsubscribe for key {}: {}", key, err);
                    err
                })?;
            Ok(())
        } else {
            Err(NotifyKeyValueError::NotFound)
        }
    }
    pub async fn trace(&mut self, key: &str) -> Result<Vec<String>, NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<N>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<N>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<N>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
        if let Some(data) = self.notifiers.get_mut(key, capture_and_push).await {
            for key in data.lock().await.subscriptions.keys().cloned() {
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
}

#[cfg(test)]
mod tests {
    use crate::persist_value::FileStorage;

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
}
