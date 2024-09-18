// SPDX-License-Identifier: Apache-2.0

// NotifyKeyValue structure is a persistent key-value
// storage with ability to notify clients about changes
// made in a value. When created via new() it will try to
// load values from folder. Underlying structure is a HashMap
// and designed to be access synchronously.

use std::fs;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::notifier::{Message, Notifier, NotifierError, WriteStream};
use crate::persist_value::PersistValue;
use crate::trie::{Trie, TrieNode};
use std::fmt;
use std::net::SocketAddr;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum NotifyKeyValueError {
    NoError,
    NotFound,
    NotifierError(NotifierError),
    IoError(std::io::Error),
}

impl NotifyKeyValueError {
    pub fn to_http_status(&self) -> http::StatusCode {
        match self {
            NotifyKeyValueError::NotFound => http::StatusCode::NOT_FOUND,
            NotifyKeyValueError::NoError => http::StatusCode::OK,
            NotifyKeyValueError::NotifierError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            NotifyKeyValueError::IoError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<NotifierError> for NotifyKeyValueError {
    fn from(error: NotifierError) -> Self {
        NotifyKeyValueError::NotifierError(error)
    }
}

impl From<std::io::Error> for NotifyKeyValueError {
    fn from(error: std::io::Error) -> Self {
        NotifyKeyValueError::IoError(error)
    }
}

impl fmt::Display for NotifyKeyValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyKeyValueError::NotFound => write!(f, "Not Found"),
            NotifyKeyValueError::NoError => write!(f, "No Error"),
            NotifyKeyValueError::NotifierError(e) => write!(f, "Notifier error {}", e),
            NotifyKeyValueError::IoError(e) => write!(f, "IO error {}", e),
        }
    }
}

impl std::error::Error for NotifyKeyValueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifyKeyValueError::NoError => None,
            NotifyKeyValueError::NotFound => None,
            NotifyKeyValueError::NotifierError(e) => Some(e),
            NotifyKeyValueError::IoError(e) => Some(e),
        }
    }
}

#[derive(Debug)]
struct Value {
    pv: PersistValue,
    notifier: Arc<Mutex<Notifier>>,
}

pub struct NotifyKeyValue {
    state: Trie<Value>,
    persist_path: PathBuf,
}

impl NotifyKeyValue {
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
                        let val = PersistValue::from_checkpoint(&path)?;
                        let notifier = Arc::new(Mutex::new(Notifier::new()));

                        res.state.insert(key, Value { pv: val, notifier });
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

    pub async fn put(&mut self, key: &str, value: Box<[u8]>) {
        let vector: Arc<Mutex<Vec<Arc<Mutex<Notifier>>>>> = Arc::new(Mutex::new(Vec::new()));

        let capture_and_push: Option<
            Box<
                dyn Fn(&mut TrieNode<Value>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        > = Some({
            Box::new(
                move |trie_ref: &mut TrieNode<Value>| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                    // Extract the notifier Arc from the TrieNode
                    let notifier_arc = match trie_ref.value.as_ref() {
                        Some(value) => Arc::clone(&value.notifier),
                        None => return Box::pin(async {}), // Early return if no value in trie_ref
                    };

                    let vector_clone = Arc::clone(&vector);

                    Box::pin(async move {
                        // Lock notifier's tokio mutex only for the critical section
                        {
                            let _notifier = notifier_arc.lock().await; // Lock the notifier's tokio mutex
                        } // Mutex lock scope ends here

                        // Lock the vector's tokio mutex and push the notifier arc
                        let mut vector_lock = vector_clone.lock().await;
                        vector_lock.push(notifier_arc); // No need to clone, push directly
                    })
                },
            )
        });

        if let Some(val) = self.state.get_mut(key, capture_and_push) {
            // TODO: Maybe we can use reference?
            // so that we don't have to clone
            let _ = val.pv.update(value.clone());
            let _ = val.notifier.lock().await.send_update(value).await;
        } else {
            let path = self.persist_path.join(key);
            let val = PersistValue::new(value, path).expect("TOREMOVE");
            let notifier = Arc::new(Mutex::new(Notifier::new()));

            self.state.insert(key, Value { pv: val, notifier });
        }
    }

    pub fn get(&self, key: &str) -> Vec<Arc<[u8]>> {
        self.state
            .get(key)
            .iter()
            .map(|s| Arc::clone(&s.pv.data()))
            .collect()
    }

    pub async fn delete(&mut self, key: &str) -> Result<(), NotifyKeyValueError> {
        if let Some(val) = self.state.get_mut(key, None) {
            val.notifier.lock().await.unsubscribe_all().await?;
            val.pv.delete_checkpoint()?;
        }
        self.state.remove(key);
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        key: &str,
        addr: SocketAddr,
        mut stream: WriteStream,
    ) -> Result<(), NotifierError> {
        if let Some(val) = self.state.get_mut(key, None) {
            val.notifier.lock().await.subscribe(addr, stream).await;
            Ok(())
        } else {
            // Send to client message that key was not found
            let msg = Message::NotFound;
            let json_bytes = serde_json::to_vec(&msg).unwrap();
            Notifier::send_bytes(&json_bytes, &mut stream).await
        }
    }

    pub async fn unsubscribe(&mut self, key: &str, addr: &SocketAddr) {
        if let Some(val) = self.state.get_mut(key, None) {
            match val.notifier.lock().await.unsubscribe(addr).await {
                Ok(_) => (),
                Err(e) => eprintln!("Failed to unsubscribe {}", e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::TempDir;
    use tokio;

    #[tokio::test]
    async fn test_put_and_get() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await;

        let result = nkv.get("key1");
        assert_eq!(result, vec!(Arc::from(data)));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf())?;

        let result = nkv.get("nonexistent_key");
        assert_eq!(result, Vec::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await;

        nkv.delete("key1").await?;
        let result = nkv.get("key1");
        assert_eq!(result, Vec::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf())?;

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await;

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("key1", new_data.clone()).await;

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
            let mut nkv = NotifyKeyValue::new(path.clone())?;
            nkv.put("key1", data1.clone()).await;
            nkv.put("key2", data2.clone()).await;
            nkv.put("key3", data3.clone()).await;
        }

        let nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf())?;
        let result = nkv.get("key1");
        assert_eq!(result, vec!(Arc::from(data1)));

        let result = nkv.get("key2");
        assert_eq!(result, vec!(Arc::from(data2)));

        let result = nkv.get("key3");
        assert_eq!(result, vec!(Arc::from(data3)));

        Ok(())
    }
}
