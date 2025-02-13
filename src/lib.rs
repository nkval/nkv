// SPDX-License-Identifier: Apache-2.0

// NkvClient is a structure which is used to
// communicate with Server to get, put, subscribe
// and unsubscribe to a value

pub mod errors;
pub mod flag_parser;
pub mod nkv;
pub mod notifier;
pub mod persist_value;
pub mod request_msg;
pub mod srv;
pub mod traits;
pub mod trie;

use crate::notifier::Subscriber;
use crate::request_msg::Message;
use crate::request_msg::*;

use std::collections::HashMap;
use std::fmt;
use std::str;
use tokio::net::UnixStream;
// use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;

#[derive(Debug)]
pub enum NkvClientError {
    SubscriptionNotFound(String),
}

impl fmt::Display for NkvClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NkvClientError::SubscriptionNotFound(s) => write!(f, "Subscription not found {}", s),
        }
    }
}

impl std::error::Error for NkvClientError {}

pub struct NkvClient {
    addr: String,
    client_uuid: String,
    subscriptions: HashMap<String, String>,
}

pub trait Subscription {
    fn handle_update(self, msg: Message);
}

impl NkvClient {
    // When you see this UUID returned by client it means request
    // was not sent to server, for example when you try to
    // unsubscribe from non-existent subscription
    const LOCAL_UUID: &'static str = "0";

    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            client_uuid: Self::uuid(),
            subscriptions: HashMap::new(),
        }
    }

    fn uuid() -> String {
        "rust-nkv-client-".to_string() + &Uuid::new_v4().to_string()
    }

    pub async fn get(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Get(BaseMessage {
            id: Self::uuid(),
            client_uuid: self.client_uuid.clone(),
            key,
        });
        self.send_request(&req).await
    }

    pub async fn put(&mut self, key: String, val: Box<[u8]>) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Put(PutMessage {
            base: BaseMessage {
                id: Self::uuid(),
                client_uuid: self.client_uuid.clone(),
                key,
            },
            value: val,
        });
        self.send_request(&req).await
    }

    pub async fn delete(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Delete(BaseMessage {
            id: Self::uuid(),
            client_uuid: self.client_uuid.clone(),
            key,
        });
        self.send_request(&req).await
    }

    pub async fn unsubscribe(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        if let Some(req_id) = self.subscriptions.get(&key) {
            let req = ServerRequest::Unsubscribe(BaseMessage {
                id: req_id.to_string(),
                client_uuid: self.client_uuid.clone(),
                key,
            });
            self.send_request(&req).await
        } else {
            return Ok(ServerResponse::Base(BaseResp {
                id: Self::LOCAL_UUID.to_string(),
                status: false,
            }));
        }
    }

    pub async fn subscribe(
        &mut self,
        key: String,
        hdlr: Box<dyn Fn(Message) + Send>,
    ) -> tokio::io::Result<ServerResponse> {
        // we subscribe only once during client lifetime
        if let Some(req_id) = self.subscriptions.get(&key) {
            return Ok(ServerResponse::Base(BaseResp {
                id: req_id.to_string(),
                status: false,
            }));
        }

        let (mut subscriber, mut rx) = Subscriber::new(&self.addr, &key, &self.client_uuid);

        tokio::spawn(async move {
            // TODO: stop when cancleed
            subscriber.start().await;
        });

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        let val = rx.borrow().to_owned();
                        hdlr(val);
                    }
                }
            }
        });

        // server uses request id to differenciate between
        // subscriptions. So we need it when unsubscribing
        self.subscriptions.insert(key, Self::uuid());

        Ok(ServerResponse::Base(BaseResp {
            id: self.client_uuid.clone(),
            status: true,
        }))
    }

    async fn send_request(&mut self, request: &ServerRequest) -> tokio::io::Result<ServerResponse> {
        let stream = UnixStream::connect(&self.addr).await?;
        let (reader, mut writer) = stream.into_split();

        let mut buf_reader = BufReader::new(reader);

        let msg = format!("{}\n", request.to_string());
        writer.write_all(msg.as_bytes()).await?;
        writer.flush().await?;

        let mut line = String::new();
        buf_reader
            .read_line(&mut line)
            .await
            .map_err(|_| tokio::io::Error::new(tokio::io::ErrorKind::Other, "read error"))?;

        let response: ServerResponse = line.trim_end().parse().map_err(|_| {
            tokio::io::Error::new(
                tokio::io::ErrorKind::Other,
                "failed to parse message to UTF-8 String",
            )
        })?;

        Ok(response)
    }
}
