pub mod nkv;
pub mod notifier;
mod persist_value;
pub mod request_msg;

use crate::notifier::{Message, Subscriber};
use crate::request_msg::*;

use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::watch;

pub struct NkvClient {
    addr: String,
    pub subscriptions: HashMap<String, watch::Receiver<Message>>,
    pub rx: Option<watch::Receiver<Message>>,
}

impl NkvClient {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            subscriptions: HashMap::new(),
            rx: None,
        }
    }

    pub async fn get(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Get(BaseMessage { id: 0, key });
        self.send_request(&req).await
    }

    pub async fn put(&mut self, key: String, val: Box<[u8]>) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Put(PutMessage {
            base: BaseMessage { id: 0, key },
            value: val,
        });
        self.send_request(&req).await
    }

    pub async fn delete(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        let req = ServerRequest::Delete(BaseMessage { id: 0, key });
        self.send_request(&req).await
    }

    pub async fn subscribe(&mut self, key: String) -> tokio::io::Result<ServerResponse> {
        // we subscribe only once during client lifetime
        if self.subscriptions.contains_key(&key) {
            return Ok(ServerResponse::Base(BaseResp {
                id: 0,
                status: http::StatusCode::OK,
                message: "ALREADY SUBSCRIBED".to_string(),
            }));
        }

        let (mut subscriber, rx) = Subscriber::new(&self.addr, &key);

        tokio::task::spawn(async move {
            subscriber.start().await;
        });

        self.subscriptions.insert(key, rx);

        Ok(ServerResponse::Base(BaseResp {
            id: 0,
            status: http::StatusCode::OK,
            message: "OK".to_string(),
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
