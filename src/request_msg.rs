use http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct BaseMessage {
    pub id: String,
    pub key: String,
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
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct BaseResp {
    pub id: String,

    #[serde(with = "http_serde::status_code")]
    pub status: StatusCode,
    pub message: String,
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
    pub data: Vec<u8>,
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
