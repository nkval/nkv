// SPDX-License-Identifier: Apache-2.0

// Server gives you an asynchronous access to a NotifyKeyValue storage.
// You can get it within the same procees from same or another thread
// using channels or from another program using tcp socket, for message
// format you can check request_msg.rs

use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, info_span, Instrument};

use crate::errors::NotifyKeyValueError;
use crate::nkv::NkvCore;
use crate::notifier::{Notifier, TcpWriter};
use crate::persist_value::FileStorage;
use crate::request_msg::{self, BaseMessage, PutMessage, ServerRequest, ServerResponse};

pub struct PutMsg {
    pub key: String,
    pub value: Box<[u8]>,
    pub resp_tx: mpsc::Sender<NotifyKeyValueError>,
}

pub struct NkvGetResp {
    err: NotifyKeyValueError,
    value: HashMap<String, Arc<[u8]>>,
}

pub struct GetMsg {
    pub key: String,
    pub resp_tx: mpsc::Sender<NkvGetResp>,
}

pub struct BaseMsg {
    pub key: String,
    pub uuid: String,
    pub resp_tx: mpsc::Sender<NotifyKeyValueError>,
}

pub struct SubMsg {
    key: String,
    pub uuid: String,
    writer: TcpWriter,
    resp_tx: mpsc::Sender<NotifyKeyValueError>,
}

// Note that IP addr is locked only when serve is called
pub struct Server {
    addr: String,
    put_tx: mpsc::UnboundedSender<PutMsg>,
    get_tx: mpsc::UnboundedSender<GetMsg>,
    del_tx: mpsc::UnboundedSender<BaseMsg>,
    sub_tx: mpsc::UnboundedSender<SubMsg>,
    unsub_tx: mpsc::UnboundedSender<BaseMsg>,
    cancel_rx: oneshot::Receiver<()>,
}

impl Server {
    pub async fn new(
        addr: String,
        path: std::path::PathBuf,
    ) -> std::io::Result<(Self, oneshot::Sender<()>)> {
        let (put_tx, mut put_rx) = mpsc::unbounded_channel::<PutMsg>();
        let (get_tx, mut get_rx) = mpsc::unbounded_channel::<GetMsg>();
        let (del_tx, mut del_rx) = mpsc::unbounded_channel::<BaseMsg>();
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<SubMsg>();
        let (unsub_tx, mut unsub_rx) = mpsc::unbounded_channel::<BaseMsg>();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (usr_cancel_tx, mut usr_cancel_rx) = oneshot::channel();

        let storage = FileStorage::new(path)?;
        let mut nkv = NkvCore::new(storage)?;

        let socket_path = Path::new(&addr);

        if let Ok(metadata) = fs::metadata(socket_path).await {
            let file_type = metadata.file_type();
            if file_type.is_socket() {
                fs::remove_file(socket_path).await?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Path exists but is not a Unix socket",
                ));
            }
        }

        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let srv = Self {
            addr,
            put_tx,
            get_tx,
            del_tx,
            sub_tx,
            unsub_tx,
            cancel_rx,
        };
        let mut notifiers: HashMap<String, Notifier> = HashMap::new();

        // Spawn task to handle Asynchronous access to notify key value
        // storage via channels
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(req) = put_rx.recv() => {
                        let err = match nkv.put(&req.key, req.value).await {
                            Ok(_) => NotifyKeyValueError::NoError,
                            Err(e) => e,
                        };
                        let _ = req.resp_tx.send(err).await;
                    }
                    Some(req) = get_rx.recv() => {
                        let vals = nkv.get(&req.key);
                        if vals.len() > 0 {
                            let _ = req.resp_tx.send(NkvGetResp {
                                value: vals,
                                err: NotifyKeyValueError::NoError
                            }).await;
                        } else {
                            let _ = req.resp_tx.send(NkvGetResp {
                                value: HashMap::new(),
                                err: NotifyKeyValueError::NotFound
                            }).await;
                        }
                   }
                   Some(req) = del_rx.recv() => {
                       let err = match nkv.delete(&req.key).await {
                           Ok(_) => NotifyKeyValueError::NoError,
                           Err(e) => e,
                       };
                       let _ = req.resp_tx.send(err).await;
                   }
                   Some(req) = sub_rx.recv() => {
                       let mut err = NotifyKeyValueError::NoError;
                       if let Some(n) = notifiers.get_mut(&req.key) {
                           n.subscribe(req.uuid, req.writer).await;
                       } else {
                           match nkv.subscribe(&req.key, req.uuid.clone()).await {
                               Err(e) => { err = e }
                               Ok(rx) => {
                                   let n = Notifier::new(rx);
                                   n.subscribe(req.uuid, req.writer).await;
                                   notifiers.insert(req.key, n);
                               }
                           }
                       }
                       let _ = req.resp_tx.send(err).await;
                   }
                   Some(req) = unsub_rx.recv() => {
                       let err = match nkv.unsubscribe(&req.key, req.uuid).await {
                           Ok(_) => NotifyKeyValueError::NoError,
                           Err(e) => e,
                       };
                       let _ = req.resp_tx.send(err).await;
                   }

                   _ = &mut usr_cancel_rx => {
                       _ = cancel_tx.send(());
                       return;
                   }

                   else => { return; }
                }
            }
        });

        Ok((srv, usr_cancel_tx))
    }

    pub async fn serve(&mut self) {
        let listener = UnixListener::bind(&self.addr).unwrap();
        info!("server is listening on {}", self.addr);
        loop {
            let put_tx = self.put_tx();
            let get_tx = self.get_tx();
            let del_tx = self.del_tx();
            let sub_tx = self.sub_tx();
            let unsub_tx = self.unsub_tx();

            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    tokio::spawn({
                        Self::handle_request(stream,
                        put_tx.clone(),
                        get_tx.clone(),
                        del_tx.clone(),
                        sub_tx.clone(),
                        unsub_tx.clone()
                    )});
                }
                _ = &mut self.cancel_rx => {
                    return;
                }
            }
        }
    }

    async fn handle_request(
        stream: tokio::net::UnixStream,
        put_tx: mpsc::UnboundedSender<PutMsg>,
        get_tx: mpsc::UnboundedSender<GetMsg>,
        del_tx: mpsc::UnboundedSender<BaseMsg>,
        sub_tx: mpsc::UnboundedSender<SubMsg>,
        unsub_tx: mpsc::UnboundedSender<BaseMsg>,
    ) {
        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(read_half);
        let writer = BufWriter::new(write_half);

        let mut line = String::new();
        if reader.read_line(&mut line).await.is_err() {
            error!("failed to read request");
            return;
        }

        let req: ServerRequest = match line.trim_end().parse() {
            Ok(r) => r,
            Err(e) => {
                error!("failed to convert {} to Request: {}", line.trim_end(), e);
                return;
            }
        };

        // So that we can trace requests across threads for the given message id
        let msg_id = match req {
            ServerRequest::Put(ref msg) => msg.base.id.clone(),
            ServerRequest::Get(ref msg)
            | ServerRequest::Delete(ref msg)
            | ServerRequest::Subscribe(ref msg)
            | ServerRequest::Unsubscribe(ref msg) => msg.id.clone(),
        };
        let span = info_span!("handle_request", msg_id=%msg_id);
        let _guard = span.enter();
        debug!("handling request {:?}", &req);
        match req {
            ServerRequest::Put(msg) => {
                Self::handle_put(writer, put_tx, msg)
                    .instrument(span.clone())
                    .await;
            }
            ServerRequest::Get(msg) => {
                Self::handle_get(writer, get_tx, msg)
                    .instrument(span.clone())
                    .await;
            }
            ServerRequest::Delete(msg) => {
                Self::handle_delete(writer, del_tx, msg)
                    .instrument(span.clone())
                    .await;
            }
            ServerRequest::Subscribe(msg) => {
                Self::handle_sub(writer, sub_tx, msg)
                    .instrument(span.clone())
                    .await;
            }
            ServerRequest::Unsubscribe(msg) => {
                Self::handle_unsub(writer, unsub_tx, msg)
                    .instrument(span.clone())
                    .await;
            }
        }
    }

    async fn handle_put(writer: TcpWriter, nkv_tx: mpsc::UnboundedSender<PutMsg>, msg: PutMessage) {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        let res = nkv_tx.send(PutMsg {
            key: msg.base.key,
            value: msg.value,
            resp_tx,
        });
        if let Err(e) = res {
            error!("failed sending message to channel: {}", e);
            let resp = request_msg::BaseResp {
                id: msg.base.id,
                status: false, // failed to send request
            };
            return Self::write_response(ServerResponse::Base(resp), writer).await;
        }

        let nkv_resp = resp_rx.recv().await.unwrap();
        let status = match nkv_resp {
            NotifyKeyValueError::NoError => true,
            _ => false,
        };
        let resp = request_msg::BaseResp {
            id: msg.base.id,
            status,
        };
        Self::write_response(ServerResponse::Base(resp), writer).await;
    }

    async fn handle_get(
        writer: TcpWriter,
        nkv_tx: mpsc::UnboundedSender<GetMsg>,
        msg: BaseMessage,
    ) {
        let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
        let res = nkv_tx.send(GetMsg {
            key: msg.key,
            resp_tx: get_resp_tx,
        });
        if let Err(e) = res {
            error!("failed sending message to channel: {}", e);
            let resp = request_msg::BaseResp {
                id: msg.id,
                status: false, // failed to send request
            };
            return Self::write_response(ServerResponse::Base(resp), writer).await;
        }

        let nkv_resp = get_resp_rx.recv().await.unwrap();
        let data: HashMap<String, Vec<u8>> = nkv_resp
            .value
            .into_iter()
            .map(|(k, v)| (k, v.as_ref().to_vec()))
            .collect();
        let status = match nkv_resp.err {
            NotifyKeyValueError::NoError => true,
            _ => false,
        };
        let resp = request_msg::DataResp {
            base: request_msg::BaseResp { id: msg.id, status },
            data,
        };
        Self::write_response(ServerResponse::Data(resp), writer).await;
    }

    async fn handle_delete(
        writer: TcpWriter,
        nkv_tx: mpsc::UnboundedSender<BaseMsg>,
        msg: BaseMessage,
    ) {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        let res = nkv_tx.send(BaseMsg {
            key: msg.key,
            uuid: msg.client_uuid,
            resp_tx,
        });
        if let Err(e) = res {
            error!("failed sending message to channel: {}", e);
            let resp = request_msg::BaseResp {
                id: msg.id,
                status: false, // failed to send request
            };
            return Self::write_response(ServerResponse::Base(resp), writer).await;
        }

        let nkv_resp = resp_rx.recv().await.unwrap();
        let status = match nkv_resp {
            NotifyKeyValueError::NoError => true,
            _ => false,
        };
        let resp = request_msg::BaseResp { id: msg.id, status };
        Self::write_response(ServerResponse::Base(resp), writer).await;
    }

    async fn handle_sub(
        writer: TcpWriter,
        nkv_tx: mpsc::UnboundedSender<SubMsg>,
        msg: BaseMessage,
    ) {
        let (resp_tx, _) = mpsc::channel(1);
        let res = nkv_tx.send(SubMsg {
            key: msg.key,
            uuid: msg.client_uuid,
            writer,
            resp_tx,
        });
        if let Err(e) = res {
            error!("failed sending message to channel: {}", e);
        }
    }

    async fn handle_unsub(
        writer: TcpWriter,
        nkv_tx: mpsc::UnboundedSender<BaseMsg>,
        msg: BaseMessage,
    ) {
        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        let res = nkv_tx.send(BaseMsg {
            key: msg.key,
            uuid: msg.client_uuid,
            resp_tx,
        });
        if let Err(e) = res {
            error!("failed sending message to channel: {}", e);
            let resp = request_msg::BaseResp {
                id: msg.id,
                status: false, // failed to send request
            };
            return Self::write_response(ServerResponse::Base(resp), writer).await;
        }

        let nkv_resp = resp_rx.recv().await.unwrap();
        let status = match nkv_resp {
            NotifyKeyValueError::NoError => true,
            _ => false,
        };
        let resp = request_msg::BaseResp { id: msg.id, status };
        Self::write_response(ServerResponse::Base(resp), writer).await;
    }

    async fn write_response(reply: ServerResponse, mut writer: TcpWriter) {
        let msg = format!("{}\n", reply.to_string());
        if let Err(e) = writer.write_all(&msg.into_bytes()).await {
            error!("failed to write to socket; err = {:?}", e);
            return;
        }
        if let Err(e) = writer.flush().await {
            error!("failed to flush socket; err = {:?}", e);
            return;
        }
    }

    pub fn put_tx(&self) -> mpsc::UnboundedSender<PutMsg> {
        self.put_tx.clone()
    }
    pub fn get_tx(&self) -> mpsc::UnboundedSender<GetMsg> {
        self.get_tx.clone()
    }
    pub fn del_tx(&self) -> mpsc::UnboundedSender<BaseMsg> {
        self.del_tx.clone()
    }
    pub fn sub_tx(&self) -> mpsc::UnboundedSender<SubMsg> {
        self.sub_tx.clone()
    }
    pub fn unsub_tx(&self) -> mpsc::UnboundedSender<BaseMsg> {
        self.unsub_tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_msg::Message;
    use crate::NkvClient;
    use tempfile::TempDir;
    use tokio::{self, net::UnixStream};

    #[tokio::test]
    async fn test_server() {
        // let temp_dir = tempdir().unwrap();
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = temp_dir.path().join("socket.sock");
        // let url = temp_socket.to_str().unwrap();
        // let url = "/tmp/nkv-sock-test-srv";

        let (mut srv, _cancel) = Server::new(
            url.to_str().unwrap().to_string(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap();

        let put_tx = srv.put_tx();
        let get_tx = srv.get_tx();
        let del_tx = srv.del_tx();
        let sub_tx = srv.sub_tx();

        tokio::spawn(async move {
            srv.serve().await;
        });

        let value: Box<[u8]> = Box::new([1, 2, 3, 4, 5]);
        let key = "key1".to_string();
        let (resp_tx, mut resp_rx) = mpsc::channel(1);

        let _ = put_tx.send(PutMsg {
            key: key.clone(),
            value: value.clone(),
            resp_tx: resp_tx.clone(),
        });

        let message = resp_rx.recv().await.unwrap();
        assert!(matches!(message, NotifyKeyValueError::NoError));

        let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert!(matches!(got.err, NotifyKeyValueError::NoError));

        let mut expected: HashMap<String, Arc<[u8]>> = HashMap::new();
        expected.insert(key.clone(), Arc::from(value));
        assert_eq!(got.value, expected);

        // create sub
        let stream = UnixStream::connect(&url).await.unwrap();
        let (_, write) = tokio::io::split(stream);
        let writer = BufWriter::new(write);
        let uuid = "MY_AWESOME_UUID".to_string();

        let _ = sub_tx.send(SubMsg {
            key: key.clone(),
            resp_tx: resp_tx.clone(),
            uuid: uuid.clone(),
            writer,
        });
        let got = resp_rx.recv().await.unwrap();
        assert!(matches!(got, NotifyKeyValueError::NoError));

        let _ = del_tx.send(BaseMsg {
            key: key.clone(),
            uuid: uuid.clone(),
            resp_tx: resp_tx.clone(),
        });
        let got = resp_rx.recv().await.unwrap();
        assert!(matches!(got, NotifyKeyValueError::NoError));

        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert!(matches!(got.err, NotifyKeyValueError::NotFound));
    }

    #[tokio::test]
    async fn test_client_server() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = temp_dir.path().join("socket.sock");

        let (mut srv, _cancel) = Server::new(
            url.to_str().unwrap().to_string(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            srv.serve().await;
        });

        // Give time for server to get up
        // TODO: need to create a notification channel
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let mut client = NkvClient::new(url.to_str().unwrap());

        let value: Box<[u8]> = Box::new([9, 7, 3, 4, 5]);
        let key = "test_2_key1".to_string();

        let resp = client.put(key.clone(), value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: true,
            })
        );

        let get_resp = client.get(key.clone()).await.unwrap();
        let mut data: HashMap<String, Vec<u8>> = HashMap::new();
        data.insert(key.clone(), value.to_vec());
        assert_eq!(
            get_resp,
            request_msg::ServerResponse::Data(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: "0".to_string(),
                    status: true,
                },
                data,
            })
        );

        let err_get_resp = client.get("non-existent-key".to_string()).await.unwrap();
        assert_eq!(
            err_get_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: false,
            })
        );

        let (tx, mut rx) = mpsc::channel(1);
        let send_to_channel = Box::new(move |value| {
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(value).await.unwrap();
            });
        });

        // we can subscribe to non existent key
        let sub_resp = client
            .subscribe("non-existent-key".to_string(), send_to_channel.clone())
            .await
            .unwrap();
        assert_eq!(
            sub_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: true,
            })
        );
        if let Some(val) = rx.recv().await {
            assert_eq!(val, Message::Hello);
        } else {
            panic!("Expected value");
        }

        let unsub_resp = client
            .unsubscribe("non-existent-key".to_string())
            .await
            .unwrap();
        assert_eq!(
            unsub_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: true,
            })
        );
        if let Some(val) = rx.recv().await {
            assert_eq!(
                val,
                Message::Close {
                    key: "non-existent-key".to_string()
                }
            );
        } else {
            panic!("Expected value");
        }

        let _sub_resp = client
            .subscribe(key.clone(), send_to_channel.clone())
            .await
            .unwrap();
        if let Some(val) = rx.recv().await {
            assert_eq!(val, Message::Hello);
        } else {
            panic!("Expected value");
        }

        let new_value: Box<[u8]> = Box::new([42, 0, 1, 0, 1]);
        let resp = client.put(key.clone(), new_value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: true,
            })
        );

        if let Some(Message::Update { key: _, value }) = rx.recv().await {
            assert_eq!(value, new_value);
        } else {
            panic!("Expected value");
        }

        // Check if we delete value it'll automatically unsubscribe
        // all of the clients
        let _sub_resp = client
            .subscribe(key.clone(), send_to_channel.clone())
            .await
            .unwrap();
        let del_resp = client.delete(key.clone()).await.unwrap();
        assert_eq!(
            del_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: true,
            })
        );
        if let Some(val) = rx.recv().await {
            assert_eq!(val, Message::Close { key: key.clone() });
        } else {
            panic!("Expected value");
        }

        let del_get_resp = client.get(key.clone()).await.unwrap();
        assert_eq!(
            del_get_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: false,
            })
        );
    }
}
