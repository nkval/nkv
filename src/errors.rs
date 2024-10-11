// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use crate::nkv::NotificationError;

#[derive(Debug)]
pub enum NotifierError {
    FailedToWriteMessage(tokio::io::Error),
    FailedToFlushMessage(tokio::io::Error),
    IoError(std::io::Error),
    SubscribtionNotFound,
}

impl fmt::Display for NotifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifierError::FailedToWriteMessage(e) => write!(f, "Failed to Write Message: {}", e),
            NotifierError::FailedToFlushMessage(e) => write!(f, "Failed to Flush Message: {}", e),
            NotifierError::IoError(e) => write!(f, "IoError: {}", e),
            NotifierError::SubscribtionNotFound => write!(f, "Subscription not found"),
        }
    }
}

impl std::error::Error for NotifierError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifierError::FailedToWriteMessage(e) => Some(e),
            NotifierError::FailedToFlushMessage(e) => Some(e),
            NotifierError::IoError(e) => Some(e),
            NotifierError::SubscribtionNotFound => None,
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

impl NotifyKeyValueError {
    pub fn to_http_status(&self) -> http::StatusCode {
        match self {
            NotifyKeyValueError::NotFound => http::StatusCode::NOT_FOUND,
            NotifyKeyValueError::NoError => http::StatusCode::OK,
            NotifyKeyValueError::NotificationError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
            NotifyKeyValueError::IoError(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
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
