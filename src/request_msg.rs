// SPDX-License-Identifier: Apache-2.0

// This file contains common messages which are
// used between NkvClient and Server

use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Deserialize, Serialize)]
pub struct BaseMessage {
    pub id: String,
    pub key: String,
    pub client_uuid: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PutMessage {
    #[serde(flatten)]
    pub base: BaseMessage,
    pub value: Box<[u8]>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ServerRequest {
    Put(PutMessage),
    Get(BaseMessage),
    Delete(BaseMessage),
    Subscribe(BaseMessage),
    Unsubscribe(BaseMessage),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct BaseResp {
    pub id: String,

    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub message: String,
}

impl fmt::Display for BaseResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Id: {}\nStatus: {}\nMessage: {}",
            self.id, self.status, self.message
        )
    }
}

impl PartialEq for BaseResp {
    fn eq(&self, other: &Self) -> bool {
        // Ignoring id intentionally
        self.status == other.status && self.message == other.message
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DataResp {
    #[serde(flatten)]
    pub base: BaseResp,
    pub data: Vec<Vec<u8>>,
}

impl fmt::Display for DataResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\n", self.base)?;
        write!(f, "Data:\n")?;

        for el in self.data.iter() {
            match String::from_utf8(el.clone()) {
                Ok(string) => write!(f, " - {}\n", string)?,
                Err(_) => write!(f, " - {:?}\n", el)?,
            }
        }
        Ok(())
    }
}

impl PartialEq for DataResp {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.data == other.data
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerResponse {
    Base(BaseResp),
    Get(DataResp),
    Put(DataResp),
    Sub(DataResp),
}

impl PartialEq for ServerResponse {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Base(lhs), Self::Base(rhs)) => lhs == rhs,
            (Self::Get(lhs), Self::Get(rhs)) => lhs == rhs,
            (Self::Put(lhs), Self::Put(rhs)) => lhs == rhs,
            (Self::Sub(lhs), Self::Sub(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl fmt::Display for ServerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Base(resp) => write!(f, "{}", resp),
            Self::Get(resp) => write!(f, "{}", resp),
            Self::Put(resp) => write!(f, "{}", resp),
            Self::Sub(resp) => write!(f, "{}", resp),
        }
    }
}
