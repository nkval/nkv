// SPDX-License-Identifier: Apache-2.0

use super::traits::StorageEngine;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemoryStorage {
    storage: HashMap<String, Box<[u8]>>,
}

impl MemoryStorage {
    /// Create a new MemoryStorage instance
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            storage: HashMap::new(),
        })
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }
}

impl StorageEngine for MemoryStorage {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()> {
        self.storage.insert(key.to_string(), data);
        Ok(())
    }

    fn delete(&mut self, key: &str) -> std::io::Result<()> {
        match self.storage.remove(key) {
            Some(_) => Ok(()),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Key '{}' not found", key),
            )),
        }
    }

    fn get(&self, key: &str) -> Arc<[u8]> {
        match self.storage.get(key) {
            Some(data) => {
                let arc_data: Arc<[u8]> = Arc::from(data.as_ref());
                arc_data
            }
            None => Arc::new([]),
        }
    }

    fn keys(&self) -> Vec<String> {
        self.storage.keys().cloned().collect()
    }
}

// Assuming this is what your StorageEngine trait looks like
#[cfg(test)]
mod traits {
    use std::sync::Arc;

    pub trait StorageEngine {
        fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()>;
        fn delete(&mut self, key: &str) -> std::io::Result<()>;
        fn get(&self, key: &str) -> std::io::Result<Arc<[u8]>>;
        fn keys(&self) -> Vec<String>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_storage() {
        let storage = MemoryStorage::new().unwrap();
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn test_default_storage() {
        let storage = MemoryStorage::default();
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn test_put_and_get() {
        let mut storage = MemoryStorage::new().unwrap();
        let test_data = b"Hello, World!".to_vec().into_boxed_slice();
        let test_key = "test_key";

        // Put data
        storage.put(test_key, test_data.clone()).unwrap();
        assert_eq!(storage.keys().len(), 1);

        // Get data
        let retrieved_data = storage.get(test_key);
        assert_eq!(retrieved_data.as_ref(), test_data.as_ref());
    }

    #[test]
    fn test_delete_existing_key() {
        let mut storage = MemoryStorage::new().unwrap();
        let test_data = b"test data".to_vec().into_boxed_slice();
        let test_key = "delete_me";

        // Put and verify
        storage.put(test_key, test_data).unwrap();
        assert!(storage.keys().contains(&test_key.to_string()));
        assert_eq!(storage.keys().len(), 1);

        // Delete and verify
        storage.delete(test_key).unwrap();
        assert!(!storage.keys().contains(&test_key.to_string()));
        assert_eq!(storage.keys().len(), 0);
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut storage = MemoryStorage::new().unwrap();
        let result = storage.delete("nonexistent");

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(error.to_string().contains("Key 'nonexistent' not found"));
    }

    #[test]
    fn test_keys() {
        let mut storage = MemoryStorage::new().unwrap();

        // Empty storage should return empty keys
        assert!(storage.keys().is_empty());

        // Add some keys
        let keys_data = vec![("key1", b"data1"), ("key2", b"data2"), ("key3", b"data3")];

        for (key, data) in &keys_data {
            storage.put(key, data.to_vec().into_boxed_slice()).unwrap();
        }

        let mut retrieved_keys = storage.keys();
        retrieved_keys.sort(); // HashMap iteration order is not guaranteed

        let mut expected_keys: Vec<String> = keys_data.iter().map(|(k, _)| k.to_string()).collect();
        expected_keys.sort();

        assert_eq!(retrieved_keys, expected_keys);
        assert_eq!(storage.keys().len(), keys_data.len());
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut storage = MemoryStorage::new().unwrap();
        let key = "overwrite_me";
        let data1 = b"original data".to_vec().into_boxed_slice();
        let data2 = b"new data".to_vec().into_boxed_slice();

        // Put original data
        storage.put(key, data1).unwrap();
        assert_eq!(storage.keys().len(), 1);

        // Overwrite with new data
        storage.put(key, data2.clone()).unwrap();
        assert_eq!(storage.keys().len(), 1); // Should still be 1

        // Verify new data
        let retrieved = storage.get(key);
        assert_eq!(retrieved.as_ref(), data2.as_ref());
    }

    #[test]
    fn test_multiple_operations() {
        let mut storage = MemoryStorage::new().unwrap();

        // Test a sequence of operations
        storage
            .put("a", b"alpha".to_vec().into_boxed_slice())
            .unwrap();
        storage
            .put("b", b"beta".to_vec().into_boxed_slice())
            .unwrap();
        storage
            .put("c", b"gamma".to_vec().into_boxed_slice())
            .unwrap();

        assert_eq!(storage.keys().len(), 3);
        let keys = storage.keys();
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
        assert!(keys.contains(&"c".to_string()));

        // Delete one
        storage.delete("b").unwrap();
        assert_eq!(storage.keys().len(), 2);
        assert!(!storage.keys().contains(&"b".to_string()));

        // Verify remaining data
        let data_a = storage.get("a");
        let data_c = storage.get("c");
        assert_eq!(data_a.as_ref(), b"alpha");
        assert_eq!(data_c.as_ref(), b"gamma");
    }

    #[test]
    fn test_empty_data() {
        let mut storage = MemoryStorage::new().unwrap();
        let empty_data = Vec::new().into_boxed_slice();
        let key = "empty";

        storage.put(key, empty_data).unwrap();
        let retrieved = storage.get(key);
        assert!(retrieved.is_empty());
        assert_eq!(retrieved.len(), 0);
    }

    #[test]
    fn test_binary_data() {
        let mut storage = MemoryStorage::new().unwrap();
        let binary_data: Vec<u8> = (0..=255).collect();
        let boxed_data = binary_data.clone().into_boxed_slice();
        let key = "binary";

        storage.put(key, boxed_data).unwrap();
        let retrieved = storage.get(key);
        assert_eq!(retrieved.as_ref(), binary_data.as_slice());
    }

    #[test]
    fn test_large_data() {
        let mut storage = MemoryStorage::new().unwrap();
        let large_data = vec![42u8; 1_000_000]; // 1MB of data
        let boxed_data = large_data.clone().into_boxed_slice();
        let key = "large";

        storage.put(key, boxed_data).unwrap();
        let retrieved = storage.get(key);
        assert_eq!(retrieved.len(), 1_000_000);
        assert_eq!(retrieved.as_ref(), large_data.as_slice());
    }

    #[test]
    fn test_unicode_keys() {
        let mut storage = MemoryStorage::new().unwrap();
        let unicode_keys = ["🔑", "клю́ч", "鍵", "مفتاح"];

        for (i, &key) in unicode_keys.iter().enumerate() {
            let data = format!("data_{}", i).into_bytes().into_boxed_slice();
            storage.put(key, data).unwrap();
        }

        assert_eq!(storage.keys().len(), unicode_keys.len());

        for &key in &unicode_keys {
            let keys = storage.keys();
            assert!(keys.contains(&key.to_string()));
            assert!(storage.get(key).len() > 0);
        }
    }

    #[test]
    fn test_keys_ordering() {
        let mut storage = MemoryStorage::new().unwrap();

        // Add keys in specific order
        storage
            .put("z", b"last".to_vec().into_boxed_slice())
            .unwrap();
        storage
            .put("a", b"first".to_vec().into_boxed_slice())
            .unwrap();
        storage
            .put("m", b"middle".to_vec().into_boxed_slice())
            .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 3);

        // Verify all keys are present (order doesn't matter for HashMap)
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"m".to_string()));
        assert!(keys.contains(&"z".to_string()));
    }

    #[test]
    fn test_put_then_delete_all() {
        let mut storage = MemoryStorage::new().unwrap();

        // Add multiple items
        for i in 0..10 {
            let key = format!("key_{}", i);
            let data = format!("data_{}", i).into_bytes().into_boxed_slice();
            storage.put(&key, data).unwrap();
        }

        assert_eq!(storage.keys().len(), 10);

        // Delete all items
        for i in 0..10 {
            let key = format!("key_{}", i);
            storage.delete(&key).unwrap();
        }

        assert!(storage.keys().is_empty());
    }
}
