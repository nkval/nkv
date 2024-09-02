extern crate serde;
extern crate serde_json;

use crate::BaseMessage;
use crate::ServerRequest;
use core::fmt;
use serde::{Deserialize, Serialize};
use serde_json::to_vec;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{watch, Mutex};
use tokio::time::{sleep, Duration};

#[derive(Debug)]
pub enum NotifierError {
    FailedToWriteMessage(tokio::io::Error),
    FailedToFlushMessage(tokio::io::Error),
    SubscribtionNotFound,
}

impl fmt::Display for NotifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotifierError::FailedToWriteMessage(e) => write!(f, "Failed to Write Message: {}", e),
            NotifierError::FailedToFlushMessage(e) => write!(f, "Failed to Flush Message: {}", e),
            NotifierError::SubscribtionNotFound => write!(f, "Subscription not found"),
        }
    }
}

impl std::error::Error for NotifierError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NotifierError::FailedToWriteMessage(e) => Some(e),
            NotifierError::FailedToFlushMessage(e) => Some(e),
            NotifierError::SubscribtionNotFound => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Message {
    Hello,
    Update { value: Box<[u8]> },
    Close,
    NotFound,
}

pub type WriteStream = BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;

pub struct Notifier {
    clients: Arc<Mutex<HashMap<SocketAddr, WriteStream>>>,
}

impl Notifier {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, addr: SocketAddr, stream: WriteStream) {
        let mut subscribers = self.clients.lock().await;
        subscribers.insert(addr, stream);
    }

    pub async fn unsubscribe(&self, addr: &SocketAddr) -> Result<(), NotifierError> {
        let mut clients = self.clients.lock().await;
        match clients.get_mut(&addr) {
            Some(stream) => Notifier::send_bytes(&to_vec(&Message::Close).unwrap(), stream).await?,
            None => return Err(NotifierError::SubscribtionNotFound),
        }
        clients.remove(addr);
        Ok(())
    }

    pub async fn unsubscribe_all(&self) -> Result<(), NotifierError> {
        let mut clients = self.clients.lock().await;

        for (_, mut stream) in clients.drain() {
            Notifier::send_bytes(&to_vec(&Message::Close).unwrap(), &mut stream).await?;
        }

        Ok(())
    }

    pub async fn send_bytes(msg: &Vec<u8>, stream: &mut WriteStream) -> Result<(), NotifierError> {
        if let Err(e) = stream.write_all(&msg).await {
            return Err(NotifierError::FailedToWriteMessage(e));
        }
        if let Err(e) = stream.flush().await {
            return Err(NotifierError::FailedToFlushMessage(e));
        }
        Ok(())
    }

    async fn broadcast_message(&mut self, message: &Message) {
        let json_bytes = to_vec(&message).unwrap();

        let mut failed_addrs = Vec::new();
        let mut clients = self.clients.lock().await;
        for (addr, stream) in clients.iter_mut() {
            if let Err(e) = Notifier::send_bytes(&json_bytes, stream).await {
                eprintln!("broadcast message: {}", e);
                failed_addrs.push(addr.clone());
                continue;
            }
        }

        for addr in failed_addrs {
            match self.unsubscribe(&addr).await {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to unsubscribe: {}", e),
            }
        }
    }

    pub async fn send_hello(&mut self) {
        self.broadcast_message(&Message::Hello).await
    }

    pub async fn send_update(&mut self, new_value: Box<[u8]>) {
        self.broadcast_message(&Message::Update { value: new_value })
            .await;
    }

    pub async fn send_close(&mut self) {
        self.broadcast_message(&Message::Close).await
    }
}

pub struct Subscriber {
    addr: String,
    key: String,
    tx: watch::Sender<Message>,
}

impl Subscriber {
    pub fn new(addr: &str, key: &str) -> (Self, watch::Receiver<Message>) {
        let (tx, rx) = watch::channel(Message::Hello);
        (
            Self {
                addr: addr.to_string(),
                key: key.to_string(),
                tx,
            },
            rx,
        )
    }

    pub async fn start(&mut self) {
        loop {
            match self.connect().await {
                Ok(_) => println!("|---- Disconnected, trying to recconect..."),
                Err(e) => println!("|---- Failed to connect: {}", e),
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn connect(&self) -> tokio::io::Result<()> {
        let stream = TcpStream::connect(&self.addr).await?;
        let (read_half, write_half) = stream.into_split();
        let mut writer = BufWriter::new(write_half);
        let mut reader = BufReader::new(read_half);

        let req = ServerRequest::Subscribe(BaseMessage {
            id: "0".to_string(),
            key: self.key.to_string(),
        });
        let req = serde_json::to_string(&req)?;
        writer.write_all(req.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        let mut buffer = [0; 1024];
        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 {
                continue;
            }
            self.parse_message(&buffer[..n]);
        }
    }

    fn parse_message(&self, message: &[u8]) {
        match serde_json::from_slice::<Message>(message) {
            Ok(msg) => {
                _ = self.tx.send(msg);
            }
            Err(e) => {
                eprintln!("Failed to parse message: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{split, AsyncBufReadExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_send_update() {
        let srv_addr: SocketAddr = "127.0.0.1:8090"
            .parse()
            .expect("Unable to parse socket address");
        let key = "AWESOME_KEY".to_string();

        let mut notifier = Notifier::new();
        let (mut subscriber, mut rx) = Subscriber::new(srv_addr.to_string().as_str(), &key);
        let listener = TcpListener::bind(srv_addr).await.unwrap();
        let val: Box<[u8]> = "Bazinga".to_string().into_bytes().into_boxed_slice();
        let vc = val.clone();

        tokio::spawn(async move {
            subscriber.start().await;
        });

        let handle = tokio::spawn(async move {
            let (stream, addr) = listener.accept().await.unwrap();
            let (read_half, write_half) = split(stream);
            let mut reader = tokio::io::BufReader::new(read_half);
            let writer = BufWriter::new(write_half);

            let mut buffer = String::new();
            let _ = reader.read_line(&mut buffer).await;

            notifier.subscribe(addr, writer).await;
            notifier.send_update(vc.clone()).await;
            sleep(Duration::from_secs(1)).await;
        });

        handle.await.unwrap();

        assert_eq!(true, rx.changed().await.is_ok());
        let msg = rx.borrow();
        assert_eq!(*msg, Message::Update { value: val });
    }
}
