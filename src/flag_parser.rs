// SPDX-License-Identifier: Apache-2.0

// Simple structure to parse command line arguments
// in format
// --key value
// --help

use core::fmt;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub enum FlagParserError {
    DuplicateFlag(String),
    UnkownFlag(String),
}

impl fmt::Display for FlagParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlagParserError::DuplicateFlag(flag) => write!(f, "Duplicate flag {}", flag),
            FlagParserError::UnkownFlag(flag) => write!(f, "Unknown flag {}", flag),
        }
    }
}

impl std::error::Error for FlagParserError {}

#[derive(Debug)]
pub struct FlagParser {
    flags: HashMap<String, Option<String>>,
}

impl FlagParser {
    pub fn new(
        args: Vec<String>,
        allowed_flags: Option<Vec<String>>,
    ) -> Result<Self, FlagParserError> {
        let mut flags: HashMap<String, Option<String>> = HashMap::new();
        let mut iter = args.iter().peekable();
        while let Some(arg) = iter.next() {
            if let Some(flag) = arg.strip_prefix("--") {
                if flags.contains_key(flag) {
                    return Err(FlagParserError::DuplicateFlag(flag.to_string()));
                }
                if let Some(value) = iter.peek() {
                    if value.starts_with("--") {
                        // next token is new flag, we just add this one
                        flags.insert(flag.to_string(), None);
                    } else {
                        flags.insert(flag.to_string(), Some(value.to_string()));
                        iter.next();
                    }
                } else {
                    flags.insert(flag.to_string(), None);
                }
            }
        }

        if let Some(allowed) = allowed_flags {
            let allowed_set: HashSet<_> = allowed.iter().collect();
            if let Some(unknown_flag) = flags.keys().find(|&key| !allowed_set.contains(key)) {
                return Err(FlagParserError::UnkownFlag(unknown_flag.clone()));
            }
        }

        Ok(Self { flags })
    }

    // Key is not present -> None
    // Key is present but has no value -> Some(None)
    // Key is present and has value -> Some(&Some(String))
    pub fn get(&self, flag: &str) -> Option<&Option<String>> {
        self.flags.get(flag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_help_flag() {
        let parser =
            FlagParser::new(vec!["my_program".to_string(), "--help".to_string()], None).unwrap();
        assert!(parser.flags.contains_key("help"))
    }

    #[test]
    fn test_config_flag() {
        let parser = FlagParser::new(
            vec![
                "my_progam".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
            ],
            None,
        )
        .unwrap();
        assert_eq!(parser.get("config"), Some(&Some("config.toml".to_string())));
        assert_eq!(parser.get("non-existent-arg"), None);
    }

    #[test]
    fn test_multiple_flags() {
        let parser = FlagParser::new(
            vec![
                "my_progam".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
                "--help".to_string(),
                "--proxy-config".to_string(),
                "another-config.toml".to_string(),
                "--debug".to_string(),
                "--verbose".to_string(),
            ],
            None,
        )
        .unwrap();
        assert!(parser.flags.contains_key("help"));
        assert!(parser.flags.contains_key("debug"));
        assert!(parser.flags.contains_key("verbose"));
        assert_eq!(parser.get("config"), Some(&Some("config.toml".to_string())));
        assert_eq!(
            parser.get("proxy-config"),
            Some(&Some("another-config.toml".to_string()))
        );
    }

    #[test]
    fn test_duplicate_args() {
        let parser = FlagParser::new(
            vec![
                "my_progam".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
                "--help".to_string(),
                "--config".to_string(),
                "another-config.toml".to_string(),
            ],
            None,
        );
        assert!(parser.is_err());
    }

    #[test]
    fn test_allowed_flags() {
        let parser = FlagParser::new(
            vec![
                "my_progam".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
                "--help".to_string(),
            ],
            Some(vec!["config".to_string(), "help".to_string()]),
        )
        .unwrap();
        assert!(parser.flags.contains_key("help"));
        assert_eq!(parser.get("config"), Some(&Some("config.toml".to_string())));

        let parser = FlagParser::new(
            vec![
                "my_progam".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
                "--help".to_string(),
            ],
            Some(vec!["config".to_string()]),
        );
        assert!(parser.is_err());
    }
}
