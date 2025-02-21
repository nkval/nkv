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
use tracing::{debug, error};

// FileStorage implements StorageEngine and allows to save state on disk
// main feature of this structure is to allow saving files atomically,
// so there will be no corrupted files saved.
// - First, two directories in the root directory are created: ingest and digest.
// - Each file stored is written to ingest path
// - Then that file is renamed to the same location in digest path
// All of this is done in write_atomic function, since renaming is atomic operation
// in OS context, there won't be any corrupted files, we create two directories in
// specified root folder, because renaming could only be done within the _same_
// file system.
#[derive(Debug)]
pub struct FileStorage {
    root: PathBuf,
    ingest: PathBuf,
    digest: PathBuf,
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

    fn iterate_folders(
        cur_path: PathBuf,
        files: &mut Trie<PathBuf>,
        root: &PathBuf,
    ) -> io::Result<()> {
        if cur_path.is_file() {
            match cur_path.file_name() {
                Some(_) => {
                    let key = Self::path_to_key(&cur_path, root);
                    if key != "" {
                        files.insert(&key, cur_path);
                    }
                }
                None => {} // i.e. / /foo.txt/..
            }
        } else if cur_path.is_dir() {
            for entry in cur_path.read_dir()? {
                Self::iterate_folders(entry?.path(), files, root)?;
            }
        }
        Ok(())
    }

    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut res = Self {
            ingest: path.join("ingest/"),
            digest: path.join("digest/"),
            root: path,
            files: Trie::new(),
        };

        if !res.root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?} is a file, not a directory", res.root),
            ));
        }

        // delete state from previous run, files here are corrupted
        if res.ingest.exists() {
            fs::remove_dir_all(&res.ingest)?;
        }

        if !res.digest.exists() {
            fs::create_dir_all(&res.digest)?;
        }

        for entry in fs::read_dir(&res.digest)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                match path.file_name() {
                    Some(_) => {
                        let key = Self::path_to_key(&path, &res.digest);
                        if key != "" {
                            res.files.insert(&key, path);
                        }
                    }
                    None => {} // / or /foo.txt/..
                }
            } else if path.is_dir() {
                match path.file_name() {
                    Some(_) => {
                        Self::iterate_folders(path, &mut res.files, &res.digest)?;
                    }
                    None => {} // / or /foo.txt/..
                }
            }
        }

        // create directory after reading state so that we don't have to explicitly ignore it
        fs::create_dir_all(&res.ingest)?;

        Ok(res)
    }
}

impl StorageEngine for FileStorage {
    fn put(&mut self, key: &str, data: Box<[u8]>) -> std::io::Result<()> {
        let fp = self.digest.join(Self::key_to_path(key));
        debug!("file path is : {:?}", fp);
        atomic_write(&*data, &self.ingest, fp.as_path()).map_err(|err| {
            error!("atomic_write failed for {:?}: {}", &fp, err);
            err
        })?;
        self.files.insert(key, fp);
        Ok(())
    }

    fn delete(&self, key: &str) -> std::io::Result<()> {
        fs::remove_file(&self.digest.join(Self::key_to_path(key)))
    }

    fn get(&self, key: &str) -> Vec<Arc<[u8]>> {
        let mut res: Vec<Arc<[u8]>> = Vec::new();
        for files in self.files.get(key) {
            match fs::read(files) {
                Ok(data) => res.push(Arc::clone(&data.into_boxed_slice().into())),
                Err(_) => error!("failed to read {:?}", files),
            };
        }
        res
    }
}

// create a temp file, write data to it, then atomically rename
// temp file to
// we need files to be on the same file system to rename them, otherwise
// we will get cross-device link error while renaming
// WARN: this will create orphant files if someone cuts the power mid way
// for more info check README Known limitations section
fn atomic_write(data: &[u8], ingest_dir: &Path, filename: &Path) -> std::io::Result<()> {
    let mut tmp_file =
        NamedTempFile::new_in(ingest_dir.parent().unwrap_or_else(|| Path::new(".")))?;
    tmp_file.write_all(data)?;

    if let Some(parent) = filename.parent() {
        fs::create_dir_all(parent)?;
    }

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

        let file_path = dir
            .path()
            .join("digest")
            .join(FileStorage::key_to_path(key));
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
    fn test_file_storage_key_to_path_to_key() {
        let key = "keyspace1.keyspace2.keyspace3.keyspace4.keyspace5.key";
        let path = "keyspace1/keyspace2/keyspace3/keyspace4/keyspace5/key.json";
        let prefix = "some/kind/of/folders/";

        assert_eq!(path, FileStorage::key_to_path(key));
        assert_eq!(
            key,
            FileStorage::path_to_key(
                &PathBuf::from(prefix.to_string() + path),
                &PathBuf::from(prefix)
            )
        )
    }

    #[test]
    fn test_file_storage_keyspace() {
        let dir = TempDir::new().expect("Failed to create temprorary directory");
        let path = dir.path().to_path_buf();
        let mut fs = FileStorage::new(path).expect("Failed to create FileStorage");
        let original_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        let key = "keyspace1.keyspace2.keyspace3.keyspace4.keyspace5";
        fs.put(key, original_data.clone())
            .expect("Failed to put data");

        let file_path = dir
            .path()
            .join("digest")
            .join(FileStorage::key_to_path(key));
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
            let file_path = dir
                .path()
                .join("digest")
                .join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            let key = "key2";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir
                .path()
                .join("digest")
                .join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            let key = "key3";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir
                .path()
                .join("digest")
                .join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            // Be sure that creating ingest and digest directories inside FileStorage::new won't affect
            // user ability to create those folders
            let key = "ingest.key4";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir
                .path()
                .join("digest")
                .join(FileStorage::key_to_path(key));
            let data_from_file = fs::read(file_path.clone()).expect("Failed to read from filepath");
            assert_eq!(data_from_file, original_data.as_ref());

            let key = "digest.key4";
            fs.put(key, original_data.clone())
                .expect("Failed to put data");
            let file_path = dir
                .path()
                .join("digest")
                .join(FileStorage::key_to_path(key));
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
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data.clone())];
        assert_eq!(got, arc_vec);

        let got = fs.get("ingest.key4");
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data.clone())];
        assert_eq!(got, arc_vec);

        let got = fs.get("digest.key4");
        let arc_vec: Vec<Arc<[u8]>> = vec![Arc::from(original_data.clone())];
        assert_eq!(got, arc_vec);
    }
}
