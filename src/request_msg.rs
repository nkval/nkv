// SPDX-License-Identifier: Apache-2.0

// This file contains common messages which are
// used between NkvClient and Server

use base64::Engine;
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

#[derive(Debug, PartialEq, Eq)]
pub struct PutMessage {
    pub base: BaseMessage,
    pub value: Box<[u8]>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerRequest {
    Put(PutMessage),
    Get(BaseMessage),
    Delete(BaseMessage),
    Subscribe(BaseMessage),
    Unsubscribe(BaseMessage),
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
                client_uuid,
                key,
            })),
            "DEL" => Ok(Self::Delete(BaseMessage {
                id,
                client_uuid,
                key,
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

pub struct DataResp {
    pub base: BaseResp,
    pub data: Vec<Vec<u8>>,
}

impl fmt::Display for DataResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.base)?;

        for el in self.data.iter() {
            // note: use same engine in decode as well
            let engine = base64::engine::general_purpose::STANDARD;
            let value = engine.encode(el);
            write!(f, " {}", value)?
        }
        Ok(())
    }
}

impl fmt::Debug for DataResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.base)?;

        for el in self.data.iter() {
            match String::from_utf8(el.clone()) {
                Ok(s) => write!(f, " {}", s)?,
                Err(_) => write!(f, " {:?}", el)?,
            }
        }
        Ok(())
    }
}

impl FromStr for DataResp {
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

        let data = parts
            .map(|encoded| {
                let engine = base64::engine::general_purpose::STANDARD;
                engine
                    .decode(encoded)
                    .map_err(|_| SerializingError::ParseError)
            })
            .collect::<Result<Vec<Vec<u8>>, _>>()?;

        Ok(Self { base, data })
    }
}

impl PartialEq for DataResp {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.data == other.data
    }
}

pub enum ServerResponse {
    Base(BaseResp),
    Data(DataResp),
}

impl PartialEq for ServerResponse {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Base(lhs), Self::Base(rhs)) => lhs == rhs,
            (Self::Data(lhs), Self::Data(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl fmt::Display for ServerResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Base(resp) => {
                write!(f, "{}", resp)
            }
            Self::Data(resp) => {
                write!(f, "{}", resp)
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
        }
    }
}

impl FromStr for ServerResponse {
    type Err = SerializingError;

    fn from_str(input_str: &str) -> Result<Self, Self::Err> {
        match input_str.split_whitespace().count() {
            2 => {
                let base_resp: BaseResp = input_str.parse()?;
                Ok(ServerResponse::Base(base_resp))
            }
            3 => {
                let data_resp: DataResp = input_str.parse()?;
                Ok(ServerResponse::Data(data_resp))
            }
            _ => Err(SerializingError::UnknownCommand),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    Hello,
    Update { key: String, value: Box<[u8]> },
    Close { key: String },
    NotFound,
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
        assert_eq!(resp.to_string(), format!("{} OK", &id));

        let data: Vec<Vec<u8>> = vec![vec![1, 2, 3, 4]];
        let resp = ServerResponse::Data(DataResp {
            base: BaseResp {
                id: id.clone(),
                status: false,
            },
            data: data.clone(),
        });
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data[0]);
        assert_eq!(resp.to_string(), format!("{} FAILED {}", id, value))
    }

    #[test]
    fn test_server_response_from_str() {
        let id = "1".to_string();
        let expected = ServerResponse::Base(BaseResp {
            id: id.clone(),
            status: true,
        });
        let got: ServerResponse = format!("{} OK", &id).parse().unwrap();
        assert_eq!(got, expected);

        let data: Vec<Vec<u8>> = vec![vec![1, 2, 3, 4]];
        let engine = base64::engine::general_purpose::STANDARD;
        let value = engine.encode(&data[0]);
        let got: ServerResponse = format!("{} FAILED {}", id, value).parse().unwrap();
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
