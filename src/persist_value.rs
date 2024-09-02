use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
pub struct PersistValue {
    data: Arc<[u8]>,
    fp: PathBuf,
}

impl PersistValue {
    pub fn new(new_data: Box<[u8]>, filepath: PathBuf) -> std::io::Result<Self> {
        atomic_write(&*new_data, &filepath)?;

        Ok(Self {
            data: Arc::from(new_data),
            fp: filepath,
        })
    }

    pub fn from_checkpoint(filepath: &Path) -> std::io::Result<Self> {
        println!("Reading from checkpoint {:?}", filepath);
        let data = fs::read(filepath)?;

        Ok(Self {
            data: data.into_boxed_slice().into(),
            fp: filepath.to_path_buf(),
        })
    }

    pub fn update(&mut self, new_data: Box<[u8]>) -> std::io::Result<()> {
        atomic_write(&*new_data, &self.fp)?;

        self.data = new_data.into();
        Ok(())
    }

    pub fn delete_checkpoint(&self) -> std::io::Result<()> {
        fs::remove_file(&self.fp)
    }

    pub fn data(&self) -> Arc<[u8]> {
        Arc::clone(&self.data)
    }
}

impl PartialEq for PersistValue {
    fn eq(&self, other: &Self) -> bool {
        self.fp == other.fp && self.data == other.data
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
    fn test_persist_value() -> std::io::Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("testfile.dat");

        let original_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        let persist_value = PersistValue::new(original_data.clone(), file_path.clone())?;

        let data_from_file = fs::read(file_path.clone())?;
        assert_eq!(data_from_file, original_data.as_ref());

        let loaded_persist_value = PersistValue::from_checkpoint(file_path.as_path())?;

        assert_eq!(persist_value, loaded_persist_value);

        temp_dir.close()?;

        Ok(())
    }

    #[test]
    fn test_update() -> std::io::Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("testfile.dat");

        let initial_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        let mut persist_value = PersistValue::new(initial_data.clone(), file_path.clone())?;

        let new_data: Box<[u8]> = Box::new([6, 7, 8, 9, 10]);

        persist_value.update(new_data.clone())?;

        let data_from_file = fs::read(file_path.clone())?;
        assert_eq!(data_from_file, new_data.as_ref());

        assert_eq!(persist_value.data(), new_data.as_ref().into());

        temp_dir.close()?;

        Ok(())
    }

    #[test]
    fn test_delete_checkpoint() -> std::io::Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("testfile.dat");

        let initial_data: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);

        let persist_value = PersistValue::new(initial_data.clone(), file_path.clone())?;

        assert!(file_path.exists());

        persist_value.delete_checkpoint()?;

        assert!(!file_path.exists());

        temp_dir.close()?;

        Ok(())
    }
}
