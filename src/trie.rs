// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub enum TrieError {
    KeyContainsWildcard,
}

impl fmt::Display for TrieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrieError::KeyContainsWildcard => write!(f, "Key contains wildcard"),
        }
    }
}

impl std::error::Error for TrieError {}

#[derive(Debug)]
pub struct TrieNode<T> {
    pub children: HashMap<String, TrieNode<T>>,
    pub value: Option<T>,
}

impl<T> TrieNode<T> {
    fn new() -> Self {
        TrieNode {
            children: HashMap::new(),
            value: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.value.is_none()
    }

    fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, key: &str, level: usize) -> fmt::Result {
        if level > 0 {
            write!(f, "{:-<indent$}|", "", indent = (level - 1))?;
        }
        write!(f, "{:-<indent$}-> {}\n", "", key, indent = level)?;

        for (k, v) in &self.children {
            v.fmt_with_indent(f, k, level + 1)?;
        }

        Ok(())
    }
}

impl<T: PartialEq> PartialEq for TrieNode<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.children == other.children
    }
}

impl<T: Clone> Clone for TrieNode<T> {
    fn clone(&self) -> Self {
        TrieNode {
            children: self.children.clone(),
            value: self.value.clone(),
        }
    }
}

pub struct TrieIter<'a, V> {
    stack: Vec<(&'a TrieNode<V>, String)>,
}

impl<'a, V> Iterator for TrieIter<'a, V> {
    type Item = (String, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node, path)) = self.stack.pop() {
            // reverse to keep lexigraphical order
            for (ch, child) in node.children.iter().collect::<Vec<_>>().into_iter().rev() {
                let mut new_path = if path == "" {
                    path.clone()
                } else {
                    path.clone() + "."
                };
                new_path.push_str(&ch);
                self.stack.push((child, new_path));
            }

            if let Some(val) = &node.value {
                return Some((path, val));
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct Trie<T> {
    root: TrieNode<T>,
}

impl<T> Trie<T> {
    pub fn new() -> Self {
        Trie {
            root: TrieNode::new(),
        }
    }

    fn has_wildcard(key: &str) -> bool {
        key.contains("*")
    }

    pub fn iter(&self) -> TrieIter<'_, T> {
        TrieIter {
            stack: vec![(&self.root, String::new())],
        }
    }

    pub fn insert(&mut self, key: &str, value: T) -> Option<TrieError> {
        if Self::has_wildcard(key) {
            // you cannot insert with wildcard
            return Some(TrieError::KeyContainsWildcard);
        }

        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &mut self.root;

        for part in parts {
            node = node
                .children
                .entry(part.to_string())
                .or_insert_with(TrieNode::new);
        }
        node.value = Some(value);

        None
    }

    pub fn get(&self, prefix: &str) -> Vec<&T> {
        let parts: Vec<&str> = prefix.split('.').collect();
        let mut node = &self.root;

        for part in parts {
            if part == "*" {
                return self.collect_values(node);
            }
            match node.children.get(part) {
                Some(child) => node = child,
                None => return Vec::new(),
            }
        }
        if Self::has_wildcard(prefix) {
            self.collect_values(node)
        } else {
            node.value.as_ref().map_or(Vec::new(), |v| vec![v])
        }
    }

    pub async fn get_mut(
        &mut self,
        key: &str,
        op: Option<
            Box<
                dyn Fn(&mut TrieNode<T>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
    ) -> Option<&mut T> {
        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &mut self.root;

        for part in parts {
            if let Some(ref f) = op {
                f(node).await;
            }
            match node.children.get_mut(part) {
                Some(child) => node = child,
                None => return None,
            }
        }

        node.value.as_mut()
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let parts: Vec<&str> = key.split('.').collect();
        Self::delete_recursive(&mut self.root, &parts, 0)
    }

    fn delete_recursive(node: &mut TrieNode<T>, parts: &[&str], idx: usize) -> bool {
        if idx == parts.len() {
            node.value = None;
        } else {
            let part = parts[idx];
            if let Some(child) = node.children.get_mut(part) {
                if Self::delete_recursive(child, parts, idx + 1) {
                    node.children.remove(part);
                }
            }
        }
        node.is_empty()
    }

    fn collect_values<'a>(&'a self, node: &'a TrieNode<T>) -> Vec<&'a T> {
        let mut result = Vec::new();
        if let Some(ref value) = node.value {
            result.push(value);
        }
        for child in node.children.values() {
            result.extend(self.collect_values(child));
        }
        result
    }
}

impl<T: PartialEq> PartialEq for Trie<T> {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
    }
}

impl<T: Clone> Clone for Trie<T> {
    fn clone(&self) -> Self {
        Trie {
            root: self.root.clone(),
        }
    }
}

impl<T> Display for Trie<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.root.fmt_with_indent(f, "*", 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get_single_key() {
        let mut trie = Trie::new();
        let val: Box<[u8]> = Box::new([1, 2, 3]);
        trie.insert("service.function.key1", val.clone());

        let values = trie.get("service.function.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val);
    }

    #[test]
    fn test_get_with_wildcard() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);
        let val4 = Box::new([10, 11, 12]);
        let val5 = Box::new([13, 14, 15]);

        trie.insert("service.functionA.key1", val1.clone());
        trie.insert("service.functionA.key2", val2.clone());
        trie.insert("service.functionB.key1", val3.clone());
        trie.insert("service.functionB.key2", val4.clone());
        trie.insert("service.functionC.subfunction.key1", val5.clone());

        let values = trie.get("service.functionA.*");
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|v| **v == val1.clone()));
        assert!(values.iter().any(|v| **v == val2.clone()));

        let values = trie.get("service.functionB.*");
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|v| **v == val3.clone()));
        assert!(values.iter().any(|v| **v == val4.clone()));

        let values = trie.get("service.*");
        assert_eq!(values.len(), 5);
        assert!(values.iter().any(|v| **v == val1));
        assert!(values.iter().any(|v| **v == val2));
        assert!(values.iter().any(|v| **v == val3));
        assert!(values.iter().any(|v| **v == val4));
        assert!(values.iter().any(|v| **v == val5));
    }

    #[test]
    fn test_get_non_existent_key() {
        let mut trie = Trie::new();
        trie.insert("serice.function.key1", Box::new([1, 2, 3]));

        let values = trie.get("service.function.key2");
        assert!(values.is_empty());
    }

    #[test]
    fn test_get_empty_trie() {
        let trie: Trie<Box<[u8]>> = Trie::new();
        let values = trie.get("serice.function.*");
        assert!(values.is_empty());
    }

    #[test]
    fn test_insert_overwrite() {
        let mut trie = Trie::new();
        let val = Box::new([4, 5, 6]);
        trie.insert("service.function.key1", Box::new([1, 2, 3]));
        trie.insert("service.function.key1", val.clone());

        let values = trie.get("service.function.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val);
    }

    #[test]
    fn test_get_with_multiple_levels() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        trie.insert("level1.level2.level3.key1", val1.clone());
        trie.insert("level1.level2.key2", val2.clone());

        let values_level2 = trie.get("level1.level2.*");
        assert_eq!(values_level2.len(), 2);
        assert!(values_level2.iter().any(|v| **v == val1.clone()));
        assert!(values_level2.iter().any(|v| **v == val2));

        let values_level3 = trie.get("level1.level2.level3.*");
        assert_eq!(values_level3.len(), 1);
        assert_eq!(*values_level3[0], val1);
    }

    #[test]
    fn test_remove() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);
        let val4 = Box::new([10, 11, 12]);

        trie.insert("service.functionA.key1", val1.clone());
        trie.insert("service.functionB.key1", val2.clone());
        trie.insert("service.functionB.key2", val3.clone());
        trie.insert("service.functionB.subfunction.key1", val4.clone());

        assert!(!trie.remove("service.functionA.key1"));
        let values = trie.get("service.function.key1");
        assert!(values.is_empty());

        assert!(!trie.remove("service.functionB"));
        let values = trie.get("service.functionB.*");
        println!("{:?}", values);
        assert!(!values.is_empty());
    }
}
