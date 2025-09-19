// SPDX-License-Identifier: Apache-2.0

// This file contains common messages which are
// used between NkvClient and Server
// Main design decision in client-server communication
// is that we use UTF-8 valid strings, so that you can
// open socket and write to server manually, no tools needed
// With that using Rust Display traits to serialize and deserialize
// ServerRequest and Response.
// Debug formatting used to print the request in human-readable
// format

use base64::Engine;
use std::collections::HashMap;
use std::fmt;
use std::str;
use std::str::FromStr;

use crate::errors::SerializingError;

#[derive(Debug, PartialEq, Eq)]
pub struct BaseMessage {
    pub id: String,
    pub key: String,
    pub client_uuid: String,
}

#[derive(PartialEq, Eq)]
pub struct PutMessage {
    pub base: BaseMessage,
    pub value: Box<[u8]>,
}

// We are using this in debug logging,
// since data might be sensitive, we are
// not logging exact values, rather the length of data
impl fmt::Debug for PutMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} length:{}", self.base, self.value.len())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerRequest {
    Put(PutMessage),
    Get(BaseMessage),
    Delete(BaseMessage),
    Subscribe(BaseMessage),
    Unsubscribe(BaseMessage),
    Trace(BaseMessage),
    Health(BaseMessage),
    Version(BaseMessage),
}

// Since the protocol is valid UTF-8 String, we use standard Rust traits
// to convert from and to String, so that one could easily print requests
// or log them
impl fmt::Display for ServerRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Put(PutMessage { base, value }) => {
                // note: use same engine in decode as well
                let engine = base64::engine::general_purpose::STANDARD;
                let value = engine.encode(value);
                write!(
                    f,
                    "PUT {} {} {} {}",
                    base.id, base.client_uuid, base.key, value
                )
            }
            Self::Get(BaseMessage {
                id,
                key,
                client_uuid,
            }) => write!(f, "GET {} {} {}", id, client_uuid, key),
            Self::Delete(BaseMessage {
                id,
                key,
                client_uuid,
            }) => write!(f, "DEL {} {} {}", id, client_uuid, key),
            Self::Subscribe(BaseMessage {
                id,
                key,
                client_uuid,
            }) => write!(f, "SUB {} {} {}", id, client_uuid, key),
            Self::Unsubscribe(BaseMessage {
                id,
                key,
                client_uuid,
            }) => write!(f, "UNSUB {} {} {}", id, client_uuid, key),
            Self::Trace(BaseMessage {
                id,
                key,
                client_uuid,
            }) => write!(f, "TRACE {} {} {}", id, client_uuid, key),
            Self::Health(BaseMessage {
                id,
                key: _,
                client_uuid,
            }) => write!(f, "HEALTH {} {} HK", id, client_uuid),
            Self::Version(BaseMessage {
                id,
                key: _,
                client_uuid,
            }) => write!(f, "VERSION {} {} DISCARDME", id, client_uuid),
        }
    }
}

// Since the protocol is valid UTF-8 String, we use standard Rust traits
// to convert from and to String, so that one could easily print requests
// or log them
impl FromStr for ServerRequest {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        let mut parts = input_str.splitn(5, ' ');

        let request = parts
            .next()
            .ok_or(SerializingError::Missing("command".to_string()))?;
        let id = parts
            .next()
            .ok_or(SerializingError::Missing("id".to_string()))?
            .to_string();
        let client_uuid = parts
            .next()
            .ok_or(SerializingError::Missing("client-uuid".to_string()))?
            .to_string();
        let key = parts
            .next()
            .ok_or(SerializingError::Missing("key".to_string()))?
            .to_string();

        match request {
            "PUT" => {
                let value = parts
                    .next()
                    .ok_or(SerializingError::Missing("value".to_string()))?
                    .to_string();

                // note: use same engine in encode as well
                let engine = base64::engine::general_purpose::STANDARD;
                let value = if value.is_empty() {
                    Vec::new()
                } else {
                    engine.decode(value).expect("Failed to decode Base64")
                };
                Ok(Self::Put(PutMessage {
                    base: BaseMessage {
                        id,
                        client_uuid,
                        key,
                    },
                    value: value.into(),
                }))
            }
            "GET" => Ok(Self::Get(BaseMessage {
                id,
                key,
                client_uuid,
            })),
            "DEL" => Ok(Self::Delete(BaseMessage {
                id,
                key,
                client_uuid,
            })),
            "SUB" => Ok(Self::Subscribe(BaseMessage {
                id,
                client_uuid,
                key,
            })),
            "UNSUB" => Ok(Self::Unsubscribe(BaseMessage {
                id,
                client_uuid,
                key,
            })),
            "TRACE" => Ok(Self::Trace(BaseMessage {
                id,
                client_uuid,
                key,
            })),
            "HEALTH" => Ok(Self::Health(BaseMessage {
                id,
                key,
                client_uuid,
            })),
            "VERSION" => Ok(Self::Version(BaseMessage {
                id,
                key,
                client_uuid,
            })),
            _ => Err(SerializingError::UnknownCommand),
        }
    }
}

pub struct BaseResp {
    pub id: String,
    pub status: bool, // true --successful false -- failed
}

impl fmt::Display for BaseResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ", self.id)?;
        match &self.status {
            true => write!(f, "OK"),
            false => write!(f, "FAILED"),
        }
    }
}

impl fmt::Debug for BaseResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} ", self.id)?;
        match &self.status {
            true => write!(f, "OK"),
            false => write!(f, "FAILED"),
        }
    }
}

impl FromStr for BaseResp {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        let mut parts = input_str.splitn(3, ' ');
        let id = parts
            .next()
            .ok_or(SerializingError::Missing("id".to_string()))?
            .to_string();
        let status = parts
            .next()
            .ok_or(SerializingError::Missing("status".to_string()))?
            .to_string();
        let status = match status.as_str() {
            "OK" => true,
            "FAILED" => false,
            _ => return Err(SerializingError::UnknownCommand),
        };

        Ok(Self { id, status })
    }
}

impl PartialEq for BaseResp {
    fn eq(&self, other: &Self) -> bool {
        // Ignoring id intentionally
        self.status == other.status
    }
}

pub trait Marshalable: Sized {
    fn marshal(&self) -> String;
    fn debug(&self) -> String;
    fn unmarshal(input_str: &str) -> Result<Self, SerializingError>;
}

// used in Get Response
impl Marshalable for HashMap<String, Vec<u8>> {
    fn marshal(&self) -> String {
        let mut result = String::new();
        let engine = base64::engine::general_purpose::STANDARD;
        for (key, val) in self.iter() {
            let encoded_val = engine.encode(val);
            result.push_str(&format!(" {} {}", key, encoded_val));
        }
        result
    }
    fn debug(&self) -> String {
        let mut result = String::new();
        for (key, val) in self.iter() {
            match String::from_utf8(val.clone()) {
                Ok(s) => result.push_str(&format!(" {} {}", key, s)),
                Err(_) => result.push_str(&format!(" {} {:?}", key, val)),
            }
        }
        result
    }
    fn unmarshal(input_str: &str) -> Result<Self, SerializingError> {
        let parts: Vec<&str> = input_str.split_whitespace().collect();

        if parts.len() % 2 != 0 {
            return Err(SerializingError::InvalidInput);
        }

        let mut data = HashMap::new();
        let engine = base64::engine::general_purpose::STANDARD;
        for pair in parts.chunks(2) {
            let key = pair[0].to_string();
            let value = engine
                .decode(pair[1])
                .map_err(|_| SerializingError::ParseError)?;
            data.insert(key, value);
        }
        Ok(data)
    }
}

impl Marshalable for Vec<String> {
    fn marshal(&self) -> String {
        let mut result = String::new();
        for key in self.iter() {
            result.push_str(&format!(" {}", key));
        }
        result
    }
    fn debug(&self) -> String {
        let mut result = String::new();
        for key in self.iter() {
            result.push_str(&format!(" {:?}", key));
        }
        result
    }
    fn unmarshal(input_str: &str) -> Result<Self, SerializingError> {
        Ok(input_str
            .split_whitespace()
            .map(|s| s.to_string())
            .collect())
    }
}

impl Marshalable for String {
    fn marshal(&self) -> String {
        format!(" {}", self)
    }
    fn debug(&self) -> String {
        format!(" {:?}", self)
    }
    fn unmarshal(input_str: &str) -> Result<Self, SerializingError> {
        Ok(input_str.to_string())
    }
}

pub struct DataResp<T: Marshalable + PartialEq> {
    pub base: BaseResp,
    pub data: T,
}

impl<T: Marshalable + PartialEq> fmt::Display for DataResp<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.base)?;
        write!(f, "{}", self.data.marshal())
    }
}

impl<T: Marshalable + PartialEq> fmt::Debug for DataResp<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.base)?;
        write!(f, "{}", self.data.debug())
    }
}

impl<T: Marshalable + PartialEq> FromStr for DataResp<T> {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        let mut parts = input_str.split_whitespace();

        // Parse the BaseResp part
        let id = parts
            .next()
            .ok_or(SerializingError::Missing("id".to_string()))?
            .to_string();

        let status_str = parts
            .next()
            .ok_or(SerializingError::Missing("status".to_string()))?
            .to_string();
        let status = match status_str.as_str() {
            "OK" => true,
            "FAILED" => false,
            _ => return Err(SerializingError::UnknownCommand),
        };

        let base = BaseResp { id, status };

        let parts: Vec<&str> = parts.collect();

        let data = T::unmarshal(&parts.join(" "))?;

        Ok(Self { base, data })
    }
}

impl<T: Marshalable + PartialEq> PartialEq for DataResp<T> {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.data == other.data
    }
}

pub enum ServerResponse {
    Base(BaseResp),
    Data(DataResp<HashMap<String, Vec<u8>>>),
    Trace(DataResp<Vec<String>>),
    Version(DataResp<String>),
}

impl PartialEq for ServerResponse {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Base(lhs), Self::Base(rhs)) => lhs == rhs,
            (Self::Data(lhs), Self::Data(rhs)) => lhs == rhs,
            (Self::Trace(lhs), Self::Trace(rhs)) => lhs == rhs,
            (Self::Version(lhs), Self::Version(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl fmt::Display for ServerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Base(resp) => {
                write!(f, "base {}", resp)
            }
            Self::Data(resp) => {
                write!(f, "data {}", resp)
            }
            Self::Trace(resp) => {
                write!(f, "trace {}", resp)
            }
            Self::Version(resp) => {
                write!(f, "version {}", resp)
            }
        }
    }
}

impl fmt::Debug for ServerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Base(resp) => {
                write!(f, "{:?}", resp)
            }
            Self::Data(resp) => {
                write!(f, "{:?}", resp)
            }
            Self::Trace(resp) => {
                write!(f, "{:?}", resp)
            }
            Self::Version(resp) => {
                write!(f, "{:?}", resp)
            }
        }
    }
}

impl FromStr for ServerResponse {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        if let Some(first_space_index) = input_str.find(char::is_whitespace) {
            let first_word = &input_str[..first_space_index];
            let tail = input_str[first_space_index..].trim_start();

            match first_word {
                "base" => {
                    let base_resp: BaseResp = tail.parse()?;
                    return Ok(ServerResponse::Base(base_resp));
                }
                "data" => {
                    let data_resp: DataResp<HashMap<String, Vec<u8>>> = tail.parse()?;
                    return Ok(ServerResponse::Data(data_resp));
                }
                "trace" => {
                    let data_resp: DataResp<Vec<String>> = tail.parse()?;
                    return Ok(ServerResponse::Trace(data_resp));
                }
                "version" => {
                    let data_resp: DataResp<String> = tail.parse()?;
                    return Ok(ServerResponse::Version(data_resp));
                }
                _ => {}
            }
        }
        // No whitespace found — the whole string is one word
        return Err(SerializingError::Missing("type".to_string()));
    }
}

#[derive(PartialEq, Clone)]
pub enum Message {
    Hello,
    Update { key: String, value: Box<[u8]> },
    Close { key: String },
    NotFound,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Hello => write!(f, "HELLO"),
            Self::Update { key, value } => {
                write!(f, "UPDATE {} {}", key, value.len())
            }
            Self::Close { key } => write!(f, "CLOSE {}", key),
            Self::NotFound => write!(f, "NOTFOUND"),
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Hello => write!(f, "HELLO"),
            Self::Update { key, value } => {
                let engine = base64::engine::general_purpose::STANDARD;
                let value = engine.encode(&value);
                write!(f, "UPDATE {} {}", key, value)
            }
            Self::Close { key } => write!(f, "CLOSE {}", key),
            Self::NotFound => write!(f, "NOTFOUND"),
        }
    }
}

impl FromStr for Message {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        let mut parts = input_str.split_whitespace();

        let msg_type = parts
            .next()
            .ok_or(SerializingError::Missing("type".to_string()))?
            .to_string();

        match msg_type.as_str() {
            "HELLO" => Ok(Self::Hello),
            "UPDATE" => {
                let key = parts
                    .next()
                    .ok_or(SerializingError::Missing("key".to_string()))?
                    .to_string();
                let value = parts
                    .next()
                    .ok_or(SerializingError::Missing("value".to_string()))?
                    .to_string();
                let engine = base64::engine::general_purpose::STANDARD;
                let value = engine.decode(value).expect("Failed to decode Base64");

                Ok(Self::Update {
                    key,
                    value: value.into(),
                })
            }
            "CLOSE" => {
                let key = parts
                    .next()
                    .ok_or(SerializingError::Missing("key".to_string()))?
                    .to_string();

                Ok(Self::Close { key })
            }
            "NOTFOUND" => Ok(Self::NotFound),
            _ => Err(SerializingError::UnknownCommand),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_request_from_str() {
        let id = "ID".to_string();
        let client_uuid = "CLIENT-UUID".to_string();
        let key = "KEY".to_string();

        let data: Vec<u8> = vec![1, 0, 13, 12];
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data);
        let req = format!("PUT {} {} {} {}", id, client_uuid, key, value);
        let result = ServerRequest::from_str(&req).unwrap();
        assert_eq!(
            result,
            ServerRequest::Put(PutMessage {
                base: BaseMessage {
                    id: id.clone(),
                    key: key.clone(),
                    client_uuid: client_uuid.clone(),
                },
                value: data.into(),
            })
        );

        let req = format!("GET {} {} {}", id, client_uuid, key);
        let result = ServerRequest::from_str(&req).unwrap();
        assert_eq!(
            result,
            ServerRequest::Get(BaseMessage {
                id: id.clone(),
                key: key.clone(),
                client_uuid: client_uuid.clone(),
            })
        );

        let req = format!("DEL {} {} {}", id, client_uuid, key);
        let result = ServerRequest::from_str(&req).unwrap();
        assert_eq!(
            result,
            ServerRequest::Delete(BaseMessage {
                id: id.clone(),
                key: key.clone(),
                client_uuid: client_uuid.clone(),
            })
        );

        let req = format!("SUB {} {} {}", id, client_uuid, key);
        let result = ServerRequest::from_str(&req).unwrap();
        assert_eq!(
            result,
            ServerRequest::Subscribe(BaseMessage {
                id: id.clone(),
                key: key.clone(),
                client_uuid: client_uuid.clone(),
            })
        );

        let req = format!("UNSUB {} {} {}", id, client_uuid, key);
        let result = ServerRequest::from_str(&req).unwrap();
        assert_eq!(
            result,
            ServerRequest::Unsubscribe(BaseMessage {
                id: id.clone(),
                key: key.clone(),
                client_uuid: client_uuid.clone(),
            })
        );

        let req = format!("WRONG COMMAND");
        let result = ServerRequest::from_str(&req);
        assert_eq!(result.is_err(), true);

        let req = format!("GET");
        let result = ServerRequest::from_str(&req);
        assert_eq!(result.is_err(), true);

        let req = format!("GET {}", id);
        let result = ServerRequest::from_str(&req);
        assert_eq!(result.is_err(), true);

        let req = format!("GET {} {}", id, client_uuid);
        let result = ServerRequest::from_str(&req);
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_server_request_to_str() {
        let id = "ID".to_string();
        let client_uuid = "CLIENT-UUID".to_string();
        let key = "KEY".to_string();

        let data: Vec<u8> = vec![1, 0, 13, 12];
        let req = ServerRequest::Put(PutMessage {
            base: BaseMessage {
                id: id.clone(),
                key: key.clone(),
                client_uuid: client_uuid.clone(),
            },
            value: data.clone().into(),
        });
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(data);
        let expected = format!("PUT {} {} {} {}", id, client_uuid, key, value);
        assert_eq!(req.to_string(), expected);

        let req = ServerRequest::Get(BaseMessage {
            id: id.clone(),
            key: key.clone(),
            client_uuid: client_uuid.clone(),
        });
        let expected = format!("GET {} {} {}", id, client_uuid, key);
        assert_eq!(req.to_string(), expected);

        let req = ServerRequest::Delete(BaseMessage {
            id: id.clone(),
            key: key.clone(),
            client_uuid: client_uuid.clone(),
        });
        let expected = format!("DEL {} {} {}", id, client_uuid, key);
        assert_eq!(req.to_string(), expected);

        let req = ServerRequest::Subscribe(BaseMessage {
            id: id.clone(),
            key: key.clone(),
            client_uuid: client_uuid.clone(),
        });
        let expected = format!("SUB {} {} {}", id, client_uuid, key);
        assert_eq!(req.to_string(), expected);

        let req = ServerRequest::Unsubscribe(BaseMessage {
            id: id.clone(),
            key: key.clone(),
            client_uuid: client_uuid.clone(),
        });
        let expected = format!("UNSUB {} {} {}", id, client_uuid, key);
        assert_eq!(req.to_string(), expected);
    }

    #[test]
    fn test_server_response_to_str() {
        let id = "1".to_string();
        let resp = ServerResponse::Base(BaseResp {
            id: id.clone(),
            status: true,
        });
        assert_eq!(resp.to_string(), format!("base {} OK", &id));

        let key = "key".to_string();
        let mut data: HashMap<String, Vec<u8>> = HashMap::new();
        data.insert(key.clone(), vec![1, 2, 3, 4]);
        let resp = ServerResponse::Data(DataResp {
            base: BaseResp {
                id: id.clone(),
                status: false,
            },
            data: data.clone(),
        });
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data[&key]);
        assert_eq!(
            resp.to_string(),
            format!("data {} FAILED {} {}", id, key, value)
        )
    }

    #[test]
    fn test_server_response_from_str() {
        let id = "1".to_string();
        let expected = ServerResponse::Base(BaseResp {
            id: id.clone(),
            status: true,
        });
        let got: ServerResponse = format!("base {} OK", &id).parse().unwrap();
        assert_eq!(got, expected);

        let key = "key".to_string();
        let mut data: HashMap<String, Vec<u8>> = HashMap::new();
        data.insert(key.clone(), vec![1, 2, 3, 4]);
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data[&key]);
        let got: ServerResponse = format!("data {} FAILED {} {}", id, key, value)
            .parse()
            .unwrap();
        let expected = ServerResponse::Data(DataResp {
            base: BaseResp {
                id: id.clone(),
                status: false,
            },
            data: data.clone(),
        });
        assert_eq!(expected, got);
    }

    #[test]
    fn test_message_to_str() {
        assert_eq!(Message::Hello.to_string(), format!("HELLO"));

        assert_eq!(Message::NotFound.to_string(), format!("NOTFOUND"));

        let key = "MYAWESOMEKEY".to_string();
        assert_eq!(
            Message::Close { key: key.clone() }.to_string(),
            format!("CLOSE {}", key.clone())
        );

        let data: Vec<u8> = vec![1, 0, 13, 12];
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data);
        assert_eq!(
            Message::Update {
                key: key.clone(),
                value: data.into()
            }
            .to_string(),
            format!("UPDATE {} {}", key, value)
        )
    }

    #[test]
    fn test_message_from_str() {
        let got: Message = "HELLO".parse().unwrap();
        assert_eq!(got, Message::Hello);

        let got: Message = "NOTFOUND".parse().unwrap();
        assert_eq!(got, Message::NotFound);

        let key = "MYAWESOMEKEY".to_string();
        let got: Message = format!("CLOSE {}", key.clone()).parse().unwrap();
        assert_eq!(got, Message::Close { key: key.clone() });

        let data: Vec<u8> = vec![1, 0, 13, 12];
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data);
        let got: Message = format!("UPDATE {} {}", key, value).parse().unwrap();
        assert_eq!(
            got,
            Message::Update {
                key: key.clone(),
                value: data.into()
            }
        )
    }
}
