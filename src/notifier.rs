// SPDX-License-Identifier: Apache-2.0

// Notifier structure is a structure which broadcasts messages
// to all subscribed clients (Subscriber structure). Possible
// messages are following:
//
// - Hello -- is sent when the connection is established between
// Subscriber and Notifier
//
// - Update -- sent new value converted into byte array
//
// - Close -- when the Notifier is closed
//
// - NotFound -- when Subscriber is trying to subscribe to Notifier
// with a value which is not there

extern crate serde;
extern crate serde_json;

use crate::errors::NotifierError;
use crate::nkv::Message;
use crate::BaseMessage;
use crate::ServerRequest;

use serde_json::to_vec;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{sleep, Duration};

#[derive(Debug)]
struct StateBuf<T> {
    buf: [Option<T>; 2],
    idx: usize,
}

impl<T> StateBuf<T> {
    pub fn new() -> Self {
        Self {
            buf: [None, None],
            idx: 0,
        }
    }
    pub fn store(&mut self, el: T) {
        self.buf[self.idx] = Some(el)
    }
    pub fn pop(&mut self) -> Option<T> {
        let el = self.buf[self.idx].take();
        if self.idx == 0 {
            self.idx = 1;
        } else {
            self.idx = 0;
        }
        el
    }
}

pub type TcpWriter = BufWriter<tokio::io::WriteHalf<tokio::net::TcpStream>>;

#[derive(Debug)]
pub struct Notifier {
    clients: Arc<Mutex<HashMap<String, TcpWriter>>>,
}

impl Notifier {
    pub fn new(mut notifier_rx: mpsc::UnboundedReceiver<Message>) -> Self {
        let clients = Arc::new(Mutex::new(HashMap::new()));
        // use a buffer to guarantee latest state on consumers
        // for detailed information see DESIGN_DECISIONS.md
        let msg_buf = Arc::new(Mutex::new(StateBuf::new()));
        let (tx, mut rx) = watch::channel(false);

        let res = Self {
            clients: Arc::clone(&clients),
        };

        let c_msg_buf = Arc::clone(&msg_buf);
        tokio::spawn(async move {
            let clients = Arc::clone(&clients);
            // let msg_buf = Arc::clone(&msg_buf);
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        Self::send_notifications(Arc::clone(&clients), Arc::clone(&c_msg_buf)).await;
                    }

                    else => { return; }
                }
            }
        });

        tokio::spawn(async move {
            while let Some(msg) = notifier_rx.recv().await {
                msg_buf.lock().await.store(msg);
                let _ = tx.send(true);
            }
        });

        res
    }

    pub async fn subscribe(&self, uuid: String, stream: TcpWriter) {
        let mut subscribers = self.clients.lock().await;
        subscribers.insert(uuid.clone(), stream);
    }

    pub async fn unsubscribe(&self, key: String, uuid: String) -> Result<(), NotifierError> {
        let mut clients = self.clients.lock().await;
        Self::unsubscribe_impl(key, &mut clients, uuid).await
    }

    pub async fn unsubscribe_all(&self, key: &str) -> Result<(), NotifierError> {
        let mut clients = self.clients.lock().await;

        for (_, mut stream) in clients.drain() {
            Self::send_bytes(
                &to_vec(&Message::Close {
                    key: key.to_string(),
                })
                .unwrap(),
                &mut stream,
            )
            .await?;
        }

        Ok(())
    }

    async fn send_notifications(
        clients: Arc<Mutex<HashMap<String, TcpWriter>>>,
        msg_buf: Arc<Mutex<StateBuf<Message>>>,
    ) {
        let buf_val = {
            let mut buf = msg_buf.lock().await;
            buf.pop()
        };

        if let Some(val) = buf_val {
            tokio::spawn(async move {
                Self::broadcast_message(clients, &val).await;
            });
        }
    }
    async fn unsubscribe_impl(
        key: String,
        clients: &mut HashMap<String, TcpWriter>,
        uuid: String,
    ) -> Result<(), NotifierError> {
        match clients.get_mut(&uuid) {
            Some(stream) => {
                Self::send_bytes(&to_vec(&Message::Close { key }).unwrap(), stream).await?
            }
            None => return Err(NotifierError::SubscribtionNotFound),
        }
        clients.remove(&uuid);
        Ok(())
    }

    async fn send_bytes(msg: &Vec<u8>, stream: &mut TcpWriter) -> Result<(), NotifierError> {
        if let Err(e) = stream.write_all(&msg).await {
            return Err(NotifierError::FailedToWriteMessage(e));
        }
        if let Err(e) = stream.flush().await {
            return Err(NotifierError::FailedToFlushMessage(e));
        }
        Ok(())
    }

    async fn broadcast_message(clients: Arc<Mutex<HashMap<String, TcpWriter>>>, message: &Message) {
        let json_bytes = to_vec(&message).unwrap();

        let keys: Vec<String> = {
            let client_guard = clients.lock().await;
            client_guard.keys().cloned().collect()
        };

        let mut failed_addrs = Vec::new();
        for uuid in keys.iter() {
            if let Some(stream) = clients.lock().await.get_mut(uuid) {
                if let Err(e) = Self::send_bytes(&json_bytes, stream).await {
                    eprintln!("broadcast message: {}", e);
                    failed_addrs.push(uuid.clone());
                    continue;
                }
            }
        }

        let mut clients = clients.lock().await;
        for uuid in failed_addrs {
            match Self::unsubscribe_impl("failed addr".to_string(), &mut clients, uuid).await {
                Ok(_) => {}
                Err(e) => eprintln!("Failed to unsubscribe: {}", e),
            }
        }
    }
}

pub struct Subscriber {
    addr: String,
    key: String,
    uuid: String,
    tx: watch::Sender<Message>,
}

impl Subscriber {
    pub fn new(addr: &str, key: &str, uuid: &str) -> (Self, watch::Receiver<Message>) {
        let (tx, rx) = watch::channel(Message::Hello);
        (
            Self {
                addr: addr.to_string(),
                key: key.to_string(),
                uuid: uuid.to_string(),
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
            client_uuid: self.uuid.to_string(),
        });
        let req = serde_json::to_string(&req)?;
        writer.write_all(req.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        // TODO: we need to talk about max buff
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
    use crate::nkv::Notification;
    use std::net::SocketAddr;
    use tokio::io::{split, AsyncBufReadExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_send_update() {
        let srv_addr: SocketAddr = "127.0.0.1:8090"
            .parse()
            .expect("Unable to parse socket address");
        let key = "AWESOME_KEY".to_string();
        let uuid = "AWESOME_UUID".to_string();

        let mut notification = Notification::new();
        let rx = notification.subscribe("uuid1".to_string()).unwrap();

        let notifier = Notifier::new(rx);
        let (mut subscriber, mut rx) = Subscriber::new(srv_addr.to_string().as_str(), &key, &uuid);
        let listener = TcpListener::bind(srv_addr).await.unwrap();
        let val: Box<[u8]> = "Bazinga".to_string().into_bytes().into_boxed_slice();
        let vc = val.clone();

        tokio::spawn(async move {
            subscriber.start().await;
        });

        let _handle = tokio::spawn(async move {
            let (stream, _addr) = listener.accept().await.unwrap();
            let (read_half, write_half) = split(stream);
            let mut reader = tokio::io::BufReader::new(read_half);
            let writer = BufWriter::new(write_half);

            let mut buffer = String::new();
            let _ = reader.read_line(&mut buffer).await;

            let _ = notifier.subscribe(uuid, writer).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            notification
                .send_update("AWESOME_KEY".to_string(), vc.clone())
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        });

        assert_eq!(true, rx.changed().await.is_ok());
        let msg = rx.borrow();
        assert_eq!(
            *msg,
            Message::Update {
                key: "AWESOME_KEY".to_string(),
                value: val
            }
        );
    }
}
