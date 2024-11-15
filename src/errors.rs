// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use crate::nkv::NotificationError;

#[derive(Debug)]
pub enum NotifierError {
    FailedToWriteMessage(tokio::io::Error),
    FailedToFlushMessage(tokio::io::Error),
    IoError(std::io::Error),

    SubscriptionNotFound,
}

impl fmt::Display for NotifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifierError::FailedToWriteMessage(e) => write!(f, "Failed to Write Message: {}", e),
            NotifierError::FailedToFlushMessage(e) => write!(f, "Failed to Flush Message: {}", e),
            NotifierError::IoError(e) => write!(f, "IoError: {}", e),
            NotifierError::SubscriptionNotFound => write!(f, "Subscription not found"),
        }
    }
}

impl std::error::Error for NotifierError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifierError::FailedToWriteMessage(e) => Some(e),
            NotifierError::FailedToFlushMessage(e) => Some(e),
            NotifierError::IoError(e) => Some(e),
            NotifierError::SubscriptionNotFound => None,
        }
    }
}
#[derive(Debug)]
pub enum NotifyKeyValueError {
    NoError,
    NotFound,
    NotificationError(NotificationError),
    IoError(std::io::Error),
}

impl From<std::io::Error> for NotifierError {
    fn from(error: std::io::Error) -> Self {
        NotifierError::IoError(error)
    }
}

impl From<NotificationError> for NotifyKeyValueError {
    fn from(error: NotificationError) -> Self {
        NotifyKeyValueError::NotificationError(error)
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
            NotifyKeyValueError::NotificationError(e) => write!(f, "Notification error {}", e),
            NotifyKeyValueError::IoError(e) => write!(f, "IO error {}", e),
        }
    }
}

impl std::error::Error for NotifyKeyValueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifyKeyValueError::NoError => None,
            NotifyKeyValueError::NotFound => None,
            NotifyKeyValueError::NotificationError(e) => Some(e),
            NotifyKeyValueError::IoError(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum SerializingError {
    Missing(String),
    UnknownCommand,
    InvalidInput,
    ParseError,
}

impl fmt::Display for SerializingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(what) => write!(f, "Missing {}", what),
            Self::UnknownCommand => write!(f, "Unknown command"),
            Self::InvalidInput => write!(f, "Invalid input"),
            Self::ParseError => write!(f, "Failed to parse base64"),
        }
    }
}

impl std::error::Error for SerializingError {}
