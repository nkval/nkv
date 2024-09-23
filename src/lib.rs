// SPDX-License-Identifier: Apache-2.0

// NkvClient is a structure which is used to
// communicate with Server to get, put, subscribe
// and unsubscribe to a value

pub mod nkv;
pub mod notifier;
mod persist_value;
pub mod request_msg;
pub mod srv;
pub mod trie;

use crate::notifier::{Message, Subscriber};
use crate::request_msg::*;

use std::collections::HashMap;
use std::fmt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
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
    subscriptions: HashMap<String, bool>,
}

pub trait Subscription {
    fn handle_update(self, msg: Message);
}

impl NkvClient {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            subscriptions: HashMap::new(),
        }
    }

    fn uuid() -> String {
        "rust-nkv-client-".to_string() + &Uuid::new_v4().to_string()
    }

    pub async fn get(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Get(BaseMessage {
            id: Self::uuid(),
            key,
        });
        self.send_request(&req).await
    }

    pub async fn put(&mut self, key: String, val: Box<[u8]>) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Put(PutMessage {
            base: BaseMessage {
                id: Self::uuid(),
                key,
            },
            value: val,
        });
        self.send_request(&req).await
    }

    pub async fn delete(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Delete(BaseMessage {
            id: Self::uuid(),
            key,
        });
        self.send_request(&req).await
    }

    pub async fn subscribe(
        &mut self,
        key: String,
        hdlr: Box<dyn Fn(Message) + Send>,
    ) -> tokio::io::Result<ServerResponse> {
        // we subscribe only once during client lifetime
        if self.subscriptions.contains_key(&key) {
            return Ok(ServerResponse::Base(BaseResp {
                id: Self::uuid(),
                status: http::StatusCode::FOUND,
                message: "Already Subscribed".to_string(),
            }));
        }

        let (mut subscriber, mut rx) = Subscriber::new(&self.addr, &key);

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

        self.subscriptions.insert(key, true);

        Ok(ServerResponse::Base(BaseResp {
            id: Self::uuid(),
            status: http::StatusCode::OK,
            message: "Subscribed".to_string(),
        }))
    }

    async fn send_request(&mut self, request: &ServerRequest) -> tokio::io::Result<ServerResponse> {
        let stream = TcpStream::connect(&self.addr).await?;
        let (read, write) = stream.into_split();
        let mut writer = BufWriter::new(write);
        let mut reader = BufReader::new(read);

        let req = serde_json::to_string(&request)?;
        writer.write_all(req.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        let mut response_buf = String::new();

        reader.read_line(&mut response_buf).await?;
        let response: ServerResponse = serde_json::from_str(&response_buf)?;

        Ok(response)
    }
}
