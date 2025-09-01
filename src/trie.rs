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
    const WILDCARD: &'static str = "*";

    pub fn new() -> Self {
        Trie {
            root: TrieNode::new(),
        }
    }

    pub fn has_wildcard(key: &str) -> bool {
        key.contains(Self::WILDCARD)
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
            if part == Self::WILDCARD {
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
                dyn Fn(&TrieNode<T>) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
    ) -> Vec<&T> {
        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &self.root;

        for part in parts {
            if let Some(ref f) = op {
                f(node).await;
            }
            if part == Self::WILDCARD {
                return self.collect_values(node);
            }
            match node.children.get(part) {
                Some(child) => node = child,
                None => return vec![],
            }
        }

        node.value.as_ref().map_or(Vec::new(), |v| vec![v])
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let parts: Vec<&str> = key.split('.').collect();
        Self::delete_recursive(&mut self.root, &parts, 0)
    }

    fn delete_recursive(node: &mut TrieNode<T>, parts: &[&str], idx: usize) -> bool {
        if idx == parts.len() {
            // Reached the target node - only remove the value, not children
            let had_value = node.value.is_some();
            node.value = None;
            return had_value; // Return true if we actually removed something
        }

        let part = parts[idx];
        let mut removed_something = false;

        if part == Self::WILDCARD {
            // Wildcard: delete all children and their descendants
            if idx + 1 == parts.len() {
                // This is a leaf wildcard - remove all children completely
                if !node.children.is_empty() {
                    removed_something = true;
                    node.children.clear();
                }
                // Also remove value at current node if it exists
                if node.value.is_some() {
                    node.value = None;
                    removed_something = true;
                }
            } else {
                // Wildcard in the middle - recursively delete all children that match remaining path
                let mut children_to_remove = Vec::new();

                for (key, child) in node.children.iter_mut() {
                    if Self::delete_recursive(child, parts, idx + 1) {
                        removed_something = true;
                        if child.is_empty() {
                            children_to_remove.push(key.clone());
                        }
                    }
                }

                // Remove empty children
                for key in children_to_remove {
                    node.children.remove(&key);
                }
            }
        } else if let Some(child) = node.children.get_mut(part) {
            if Self::delete_recursive(child, parts, idx + 1) {
                removed_something = true;
                if child.is_empty() {
                    node.children.remove(part);
                }
            }
        }

        removed_something
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
    fn test_remove_concrete_path() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);

        trie.insert("service.functionA.key1", val1.clone());
        trie.insert("service.functionA.key2", val2.clone());
        trie.insert("service.functionB.key1", val3.clone());

        // Remove concrete path - should only remove that specific value
        assert!(trie.remove("service.functionA.key1"));

        // The removed path should be empty
        let values = trie.get("service.functionA.key1");
        assert!(values.is_empty());

        // Other paths should still exist
        let values = trie.get("service.functionA.key2");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val2);

        let values = trie.get("service.functionB.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val3);

        // Removing non-existent path should return false
        assert!(!trie.remove("service.nonexistent"));
        assert!(!trie.remove("service.functionA.key1")); // Already removed
    }

    #[test]
    fn test_remove_with_leaf_wildcard() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);
        let val4 = Box::new([10, 11, 12]);
        let val5 = Box::new([13, 14, 15]);

        trie.insert("service.functionA.key1", val1.clone());
        trie.insert("service.functionA.key2", val2.clone());
        trie.insert("service.functionA.nested.deep", val3.clone());
        trie.insert("service.functionB.key1", val4.clone());
        trie.insert("service.functionA", val5.clone()); // Value at intermediate node

        // Remove with wildcard should remove all descendants
        assert!(trie.remove("service.functionA.*"));

        // All descendants should be gone
        let values = trie.get("service.functionA.*");
        assert!(values.is_empty());

        // The intermediate node value should also be removed
        let values = trie.get("service.functionA");
        assert!(values.is_empty());

        // Other branches should remain untouched
        let values = trie.get("service.functionB.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val4);

        // Removing already cleared wildcard should return false
        assert!(!trie.remove("service.functionA.*"));
    }

    #[test]
    fn test_remove_intermediate_node() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);

        trie.insert("service.functionA.key1", val1.clone());
        trie.insert("service.functionA.key2", val2.clone());
        trie.insert("service.functionA", val3.clone()); // Value at intermediate node

        // Remove intermediate node - should only remove its value, not children
        assert!(trie.remove("service.functionA"));

        // The intermediate node should have no value
        let values = trie.get("service.functionA");
        assert!(values.is_empty());

        // But children should still exist
        let values = trie.get("service.functionA.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val1);

        let values = trie.get("service.functionA.key2");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val2);

        // Wildcard query should still return children
        let values = trie.get("service.functionA.*");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_remove_with_mid_path_wildcard() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);
        let val4 = Box::new([10, 11, 12]);

        trie.insert("service.functionA.config.key1", val1.clone());
        trie.insert("service.functionB.config.key1", val2.clone());
        trie.insert("service.functionA.data.key1", val3.clone());
        trie.insert("service.functionB.data.key1", val4.clone());

        // Remove with mid-path wildcard
        assert!(trie.remove("service.*.config.key1"));

        // Config keys should be gone
        let values = trie.get("service.functionA.config.key1");
        assert!(values.is_empty());
        let values = trie.get("service.functionB.config.key1");
        assert!(values.is_empty());

        // Data keys should remain
        let values = trie.get("service.functionA.data.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val3);
        let values = trie.get("service.functionB.data.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val4);
    }

    #[test]
    fn test_remove_deep_nested_structure() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);
        let val3 = Box::new([7, 8, 9]);

        trie.insert("a.b.c.d.e.f", val1.clone());
        trie.insert("a.b.c.d.e.g", val2.clone());
        trie.insert("a.b.c.x.y.z", val3.clone());

        // Remove part of deep structure
        assert!(trie.remove("a.b.c.d.*"));

        // Should remove all under d
        let values = trie.get("a.b.c.d.*");
        assert!(values.is_empty());

        // But other branches should remain
        let values = trie.get("a.b.c.x.y.z");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val3);
    }

    #[test]
    fn test_remove_root_level() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);
        let val2 = Box::new([4, 5, 6]);

        trie.insert("key1", val1.clone());
        trie.insert("key2", val2.clone());

        // Remove root level key
        assert!(trie.remove("key1"));

        let values = trie.get("key1");
        assert!(values.is_empty());

        let values = trie.get("key2");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], val2);
    }

    #[test]
    fn test_remove_empty_trie() {
        let mut trie: Trie<Box<[i32]>> = Trie::new();

        // Removing from empty trie should return false
        assert!(!trie.remove("any.key"));
        assert!(!trie.remove("any.*"));
    }

    #[test]
    fn test_remove_clears_empty_branches() {
        let mut trie = Trie::new();
        let val1 = Box::new([1, 2, 3]);

        trie.insert("a.b.c.d", val1.clone());

        // Remove the only leaf - should clean up empty intermediate nodes
        assert!(trie.remove("a.b.c.d"));

        // All queries should return empty
        let values = trie.get("a.b.c.d");
        assert!(values.is_empty());
        let values = trie.get("a.b.c.*");
        assert!(values.is_empty());
        let values = trie.get("a.b.*");
        assert!(values.is_empty());
        let values = trie.get("a.*");
        assert!(values.is_empty());
    }
}
