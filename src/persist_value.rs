// SPDX-License-Identifier: Apache-2.0

// PersistValue stores a value of byte arrays
// on a file system. Writing to a disk is an
// atomic operation

use crate::traits::StorageEngine;
use crate::trie::Trie;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

#[derive(Debug)]
pub struct FileStorage {
    root: PathBuf,
    files: Trie<PathBuf>,
}

impl FileStorage {
    fn key_to_path(key: &str) -> String {
        key.replace('.', "/") + ".json"
    }

    fn path_to_key(path: &PathBuf, prefix: &PathBuf) -> String {
        if let Ok(stripped) = path.strip_prefix(prefix) {
            let input_str = stripped.to_string_lossy();

            // Check if the string ends with ".json"
            if input_str.ends_with(".json") {
                // Remove the ".json" and replace slashes with dots
                let without_json = input_str.trim_end_matches(".json");
                without_json.replace('/', ".")
            } else {
                "".to_string()
            }
        } else {
            "".to_string() // If the prefix doesn't match, return None
        }
    }

    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut res = Self {
            root: path,
            files: Trie::new(),
        };

        if !res.root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?} is a file, not a directory", res.root),
            ));
        }

        for entry in fs::read_dir(&res.root)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                match path.file_name() {
                    Some(_) => {
                        let key = Self::path_to_key(&path, &res.root);
                        if key != "" {
                            res.files.insert(&key, path);
                        }
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

        if !res.root.exists() {
            fs::create_dir_all(&res.root)?;
        }

        Ok(res)
    }
}

impl StorageEngine for FileStorage {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()> {
        let fp = self.root.join(Self::key_to_path(key));
        atomic_write(&*data, &fp)?;
        self.files.insert(key, fp);
        Ok(())
    }

    fn delete(&self, key: &str) -> std::io::Result<()> {
        fs::remove_file(&self.root.join(Self::key_to_path(key)))
    }

    fn get(&self, key: &str) -> Vec<Arc<[u8]>> {
        let mut res: Vec<Arc<[u8]>> = Vec::new();
        for files in self.files.get(key) {
            match fs::read(files) {
                Ok(data) => res.push(Arc::clone(&data.into_boxed_slice().into())),
                Err(_) => eprintln!("Failed to read file"),
            };
        }
        res
    }
}

fn atomic_write(data: &[u8], filename: &Path) -> std::io::Result<()> {
    let mut tmp_file = NamedTempFile::new()?;
    tmp_file.write_all(data)?;

    let tmp_path = tmp_file.path();
    fs::rename(tmp_path, filename)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_storage_basic() {
        let dir = TempDir::new().expect("Failed to create temprorary directory");
        let path = dir.path().to_path_buf();
        let mut fs = FileStorage::new(path).expect("Failed to create FileStorage");
        let original_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        let key = "key1";
        fs.put(key, original_data.clone())
            .expect("Failed to put data");

        let file_path = dir.path().join(FileStorage::key_to_path(key));
        let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
        assert_eq!(data_from_file, original_data.as_ref());

        let got = fs.get(key);
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data)];
        assert_eq!(got, arc_vec);

        let new_data: Box<[u8]> = Box::new([6, 7, 8, 9, 10]);
        fs.put(key, new_data.clone())
            .expect("Failed to update data");

        let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
        assert_eq!(data_from_file, new_data.as_ref());

        let got = fs.get(key);
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(new_data)];
        assert_eq!(got, arc_vec);

        fs.delete(key).expect("Failed to delete value");
        assert!(!file_path.exists());

        dir.close().expect("Failed to remove temproraty directory");
    }

    #[test]
    fn test_file_storage_restore() {
        let dir = TempDir::new().expect("Failed to create temprorary directory");
        let path = dir.path().to_path_buf();
        let original_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        {
            let mut fs = FileStorage::new(path.clone()).expect("Failed to create FileStorage");
            let key = "key1";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir.path().join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            let key = "key2";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir.path().join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            let key = "key3";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir.path().join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());
        }

        let fs = FileStorage::new(path.clone()).expect("Failed to create FileStorage");

        let got = fs.get("key1");
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data.clone())];
        assert_eq!(got, arc_vec);

        let got = fs.get("key2");
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data.clone())];
        assert_eq!(got, arc_vec);

        let got = fs.get("key3");
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data)];
        assert_eq!(got, arc_vec);
    }
}
