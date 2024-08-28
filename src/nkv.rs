use std::collections::HashMap;
use std::path::PathBuf;

use std::sync::Arc;

use crate::notifier::{Message, Notifier, NotifierError, WriteStream};
use crate::persist_value::PersistValue;
use std::fmt;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum NotifyKeyValueError {
    NoError,
    NotFound,
    NotifierError(NotifierError),
}

impl NotifyKeyValueError {
    pub fn to_http_status(&self) -> http::StatusCode {
        match self {
            NotifyKeyValueError::NotFound => http::StatusCode::NOT_FOUND,
            NotifyKeyValueError::NoError => http::StatusCode::OK,
            NotifyKeyValueError::NotifierError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for NotifyKeyValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifyKeyValueError::NotFound => write!(f, "Not Found"),
            NotifyKeyValueError::NoError => write!(f, "No Error"),
            NotifyKeyValueError::NotifierError(e) => write!(f, "Notifier error {}", e),
        }
    }
}

impl std::error::Error for NotifyKeyValueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifyKeyValueError::NoError => None,
            NotifyKeyValueError::NotFound => None,
            NotifyKeyValueError::NotifierError(e) => Some(e),
        }
    }
}

struct Value {
    pv: PersistValue,
    notifier: Notifier,
}

pub struct NotifyKeyValue {
    state: HashMap<String, Value>,
    persist_path: PathBuf,
}

impl NotifyKeyValue {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self {
            state: HashMap::new(),
            persist_path: path,
        }
    }

    pub async fn put(&mut self, key: &str, value: Box<[u8]>) {
        if let Some(val) = self.state.get_mut(key) {
            // TODO: Maybe we can use reference?
            // so that we don't have to clone
            let _ = val.pv.update(value.clone());
            let _ = val.notifier.send_update(value).await;
        } else {
            let path = self.persist_path.join(key);
            let val = PersistValue::new(value, path).expect("TOREMOVE");
            let notifier = Notifier::new();

            self.state
                .insert(key.to_string(), Value { pv: val, notifier });
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<[u8]>> {
        self.state
            .get(key)
            .map(|value| Arc::clone(&value.pv.data()))
    }

    pub fn delete(&mut self, key: &str) {
        self.state.remove(key);
    }

    pub async fn subscribe(
        &mut self,
        key: &str,
        addr: SocketAddr,
        mut stream: WriteStream,
    ) -> Result<(), NotifierError> {
        if let Some(val) = self.state.get_mut(key) {
            val.notifier.subscribe(addr, stream).await;
            Ok(())
        } else {
            // Send to client message that key was not found
            let msg = Message::NotFound;
            let json_bytes = serde_json::to_vec(&msg).unwrap();
            Notifier::send_bytes(&json_bytes, &mut stream).await
        }
    }

    pub async fn unsubscribe(&mut self, key: &str, addr: &SocketAddr) {
        if let Some(val) = self.state.get_mut(key) {
            val.notifier.unsubscribe(addr).await;
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
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf());

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await;

        let result = nkv.get("key1");
        assert_eq!(result, Some(Arc::from(data)));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf());

        let result = nkv.get("nonexistent_key");
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf());

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data.clone()).await;

        nkv.delete("key1");
        let result = nkv.get("key1");
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf());

        let data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        nkv.put("key1", data).await;

        let new_data: Box<[u8]> = Box::new([5, 6, 7, 8, 9]);
        nkv.put("key1", new_data.clone()).await;

        let result = nkv.get("key1");
        assert_eq!(result, Some(Arc::from(new_data)));

        Ok(())
    }
}
