// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

#[derive(Debug, Default)]
struct TrieNode<T> {
    children: HashMap<String, TrieNode<T>>,
    value: Option<T>,
}

#[derive(Debug, Default)]
struct Trie<T> {
    root: TrieNode<T>,
}

impl<T> Trie<T>
where
    T: Clone + std::default::Default,
{
    fn insert(&mut self, key: &str, value: T) {
        let parts: Vec<&str> = key.split('.').collect();
        let mut node = &mut self.root;

        for part in parts {
            node = node
                .children
                .entry(part.to_string())
                .or_insert_with(TrieNode::default);
        }
        node.value = Some(value);
    }

    fn get(&self, prefix: &str) -> Vec<T> {
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
        self.collect_values(node)
    }

    fn collect_values(&self, node: &TrieNode<T>) -> Vec<T> {
        let mut result = Vec::new();
        if let Some(ref value) = node.value {
            result.push(value.clone());
        }
        for child in node.children.values() {
            result.extend(self.collect_values(child));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get_single_key() {
        let mut trie = Trie::default();
        trie.insert("service.function.key1", Box::new([1, 2, 3]));

        let values = trie.get("service.function.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], [1, 2, 3]);
    }

    #[test]
    fn test_get_with_wildcard() {
        let mut trie = Trie::default();
        trie.insert("service.functionA.key1", Box::new([1, 2, 3]));
        trie.insert("service.functionA.key2", Box::new([4, 5, 6]));
        trie.insert("service.functionB.key1", Box::new([7, 8, 9]));
        trie.insert("service.functionB.key2", Box::new([10, 11, 12]));
        trie.insert("service.functionC.subfunction.key1", Box::new([13, 14, 15]));

        let values = trie.get("service.functionA.*");
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|v| **v == [1, 2, 3]));
        assert!(values.iter().any(|v| **v == [4, 5, 6]));

        let values = trie.get("service.functionB.*");
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|v| **v == [7, 8, 9]));
        assert!(values.iter().any(|v| **v == [10, 11, 12]));

        let values = trie.get("service.*");
        assert_eq!(values.len(), 5);
        assert!(values.iter().any(|v| **v == [1, 2, 3]));
        assert!(values.iter().any(|v| **v == [4, 5, 6]));
        assert!(values.iter().any(|v| **v == [7, 8, 9]));
        assert!(values.iter().any(|v| **v == [10, 11, 12]));
        assert!(values.iter().any(|v| **v == [13, 14, 15]));
    }

    #[test]
    fn test_get_non_existent_key() {
        let mut trie = Trie::default();
        trie.insert("serice.function.key1", Box::new([1, 2, 3]));

        let values = trie.get("service.function.key2");
        assert!(values.is_empty());
    }

    #[test]
    fn test_get_empty_trie() {
        let trie: Trie<Box<[u8]>> = Trie::default();
        let values = trie.get("serice.function.*");
        assert!(values.is_empty());
    }

    #[test]
    fn test_insert_overwrite() {
        let mut trie = Trie::default();
        trie.insert("service.function.key1", Box::new([1, 2, 3]));
        trie.insert("service.function.key1", Box::new([4, 5, 6]));

        let values = trie.get("service.function.key1");
        assert_eq!(values.len(), 1);
        assert_eq!(*values[0], [4, 5, 6]);
    }

    #[test]
    fn test_get_with_multiple_levels() {
        let mut trie = Trie::default();
        trie.insert("level1.level2.level3.key1", Box::new([1, 2, 3]));
        trie.insert("level1.level2.key2", Box::new([4, 5, 6]));

        let values_level2 = trie.get("level1.level2.*");
        assert_eq!(values_level2.len(), 2);
        assert!(values_level2.iter().any(|v| **v == [1, 2, 3]));
        assert!(values_level2.iter().any(|v| **v == [4, 5, 6]));

        let values_level3 = trie.get("level1.level2.level3.*");
        assert_eq!(values_level3.len(), 1);
        assert_eq!(*values_level3[0], [1, 2, 3]);
    }
}
