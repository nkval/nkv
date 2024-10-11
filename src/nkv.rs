// SPDX-License-Identifier: Apache-2.0

// NotifyKeyValue structure is a persistent key-value
// storage with ability to notify clients about changes
// made in a value. When created via new() it will try to
// load values from folder. Underlying structure is a HashMap
// and designed to be access synchronously.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::{fs, future::Future, io};

use crate::errors::NotifyKeyValueError;
use crate::traits::{StoragePolicy, Value};
use crate::trie::{Trie, TrieNode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Message {
    Hello,
    Update { key: String, value: Box<[u8]> },
    Close { key: String },
    NotFound,
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Hello => write!(f, "Hello")?,
            Self::Update { key, value } => match String::from_utf8(value.to_vec()) {
                Ok(string) => write!(f, " - {} : {}\n", key, string)?,
                Err(_) => write!(f, " - {} : {:?}\n", key, value)?,
            },
            Self::Close { key } => write!(f, "{}: Close", key)?,
            Self::NotFound => write!(f, "Not Found")?,
        }
        Ok(())
    }
}
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

pub struct NkvStorage<P: StoragePolicy> {
    state: Trie<Value<P>>,
    persist_path: PathBuf,
}

impl<P: StoragePolicy> NkvStorage<P> {
    pub fn new(path: std::path::PathBuf) -> std::io::Result<Self> {
        let mut res = Self {
            state: Trie::new(),
            persist_path: path,
        };

        if !res.persist_path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?} is a file, not a directory", res.persist_path),
            ));
        }

        for entry in fs::read_dir(&res.persist_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                match path.file_name() {
                    Some(fp) => {
                        let key = fp.to_str().unwrap();
                        let value = Value {
                            pv: P::from_checkpoint(&path)?,
                            notifier: Arc::new(Mutex::new(Notification::new())),
                        };

                        res.state.insert(key, value);
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("{:?} is a directory, not a file", path),
                        ))
                    }
                }
            } else if path.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{:?} is a directory, not a file", path),
                ));
            }
        }

        Ok(res)
    }

    pub async fn put(&mut self, key: &str, value: Box<[u8]>) -> Result<(), NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<Arc<Mutex<Notification>>>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<Value<P>>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<Value<P>>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value.notifier),
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

        if let Some(val) = self.state.get_mut(key, capture_and_push).await {
            // TODO: Maybe we can use reference?
            // so that we don't have to clone
            let _ = val.pv.update(value.clone());
            // let _ = val.pv.update(value.clone());
            let _ = val
                .notifier
                .lock()
                .await
                .send_update(key.to_string(), value.clone());
        } else {
            let val = Value {
                // TODO: REMOVE UNWRAP AND ADD PROPER HANDLING
                pv: P::new(value.clone(), self.persist_path.join(key)).unwrap(),
                notifier: Arc::new(Mutex::new(Notification::new())),
            };
            self.state.insert(key, val);
        }

        let vector_lock = vector.lock().await;
        for notifier_arc in vector_lock.iter() {
            notifier_arc
                .lock()
                .await
                .send_update(key.to_string(), value.clone())?;
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Vec<Arc<[u8]>> {
        self.state
            .get(key)
            .iter()
            .map(|s| Arc::clone(&s.pv.data()))
            .collect()
    }

    pub async fn delete(&mut self, key: &str) -> Result<(), NotifyKeyValueError> {
        let vector: Arc<Mutex<Vec<Arc<Mutex<Notification>>>>> = Arc::new(Mutex::new(Vec::new()));
        let vc = Arc::clone(&vector);

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<Value<P>>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<Value<P>>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value.notifier),
                        None => return Box::pin(async {}),
                    };

                    let vector_clone = Arc::clone(&vc);

                    Box::pin(async move {
                        {
                            let _notifier = notifier_arc.lock().await;
                        }

                        let mut vector_lock = vector_clone.lock().await;
                        vector_lock.push(notifier_arc);
                    })
                },
            )
        });

        if let Some(val) = self.state.get_mut(key, capture_and_push).await {
            {
                let vec_lock = vector.lock().await;
                for n_arc in vec_lock.iter() {
                    n_arc.lock().await.send_close(key.to_string())?;
                }
            }
            val.notifier.lock().await.unsubscribe_all(key)?;
            val.pv.delete_checkpoint()?;
        }
        self.state.remove(key);
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<mpsc::UnboundedReceiver<Message>, NotifyKeyValueError> {
        if let Some(val) = self.state.get_mut(key, None).await {
            return Ok(val.notifier.lock().await.subscribe(uuid)?);
        } else {
            // Client can subscribe to a non-existent value
            let val = Value {
                pv: P::new(Box::new([]), self.persist_path.join(key))?,
                notifier: Arc::new(Mutex::new(Notification::new())),
            };
            let tx = val.notifier.lock().await.subscribe(uuid)?;
            self.state.insert(key, val);
            return Ok(tx);
        }
    }

    pub async fn unsubscribe(
        &mut self,
        key: &str,
        uuid: String,
    ) -> Result<(), NotifyKeyValueError> {
        if let Some(val) = self.state.get_mut(key, None).await {
            val.notifier
                .lock()
                .await
                .unsubscribe(key.to_string(), &uuid)?;
            Ok(())
        } else {
            Err(NotifyKeyValueError::NotFound)
        }
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
        let mut nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await?;

        let result = nkv.get("key1");
        assert_eq!(result, vec!(Arc::from(data)));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

        let result = nkv.get("nonexistent_key");
        assert_eq!(result, Vec::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await?;

        nkv.delete("key1").await?;
        let result = nkv.get("key1");
        assert_eq!(result, Vec::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await?;

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("key1", new_data.clone()).await?;

        let result = nkv.get("key1");
        assert_eq!(result, vec!(Arc::from(new_data)));

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
            let mut nkv = NkvStorage::<FileStorage>::new(path.clone())?;
            nkv.put("key1", data1.clone()).await?;
            nkv.put("key2", data2.clone()).await?;
            nkv.put("key3", data3.clone()).await?;
        }

        let nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;
        let result = nkv.get("key1");
        assert_eq!(result, vec!(Arc::from(data1)));

        let result = nkv.get("key2");
        assert_eq!(result, vec!(Arc::from(data2)));

        let result = nkv.get("key3");
        assert_eq!(result, vec!(Arc::from(data3)));

        Ok(())
    }

    #[tokio::test]
    async fn test_subsribe() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

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
        assert_eq!(result, vec!(Arc::from(new_data)));

        Ok(())
    }

    #[tokio::test]
    async fn test_unsubsribe() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NkvStorage::<FileStorage>::new(temp_dir.path().to_path_buf())?;

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
}
