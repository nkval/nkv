// SPDX-License-Identifier: Apache-2.0

// Server gives you an asynchronous access to a NotifyKeyValue storage.
// You can get it within the same procees from same or another thread
// using channels or from another program using tcp socket, for message
// format you can check request_msg.rs

use http::StatusCode;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::nkv;
use crate::notifier::WriteStream;
use crate::request_msg::{self, BaseMessage, PutMessage, ServerRequest, ServerResponse};

pub struct PutMsg {
    pub key: String,
    pub value: Box<[u8]>,
    pub resp_tx: mpsc::Sender<nkv::NotifyKeyValueError>,
}

pub struct NkvGetResp {
    err: nkv::NotifyKeyValueError,
    value: Vec<Arc<[u8]>>,
}

pub struct GetMsg {
    pub key: String,
    pub resp_tx: mpsc::Sender<NkvGetResp>,
}

pub struct BaseMsg {
    pub key: String,
    pub resp_tx: mpsc::Sender<nkv::NotifyKeyValueError>,
}

pub struct SubMsg {
    key: String,
    addr: SocketAddr,
    writer: WriteStream,
    resp_tx: mpsc::Sender<nkv::NotifyKeyValueError>,
}

// Note that IP addr is locked only when serve is called
pub struct Server {
    addr: SocketAddr,
    put_tx: mpsc::UnboundedSender<PutMsg>,
    get_tx: mpsc::UnboundedSender<GetMsg>,
    del_tx: mpsc::UnboundedSender<BaseMsg>,
    sub_tx: mpsc::UnboundedSender<SubMsg>,
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
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (usr_cancel_tx, mut usr_cancel_rx) = oneshot::channel();

        let mut nkv = nkv::NotifyKeyValue::new(path)?;
        let addr: SocketAddr = addr.parse().expect("Unable to parse addr");

        let srv = Self {
            addr,
            put_tx,
            get_tx,
            del_tx,
            sub_tx,
            cancel_rx,
        };

        // Spawn task to handle Asynchronous access to notify key value
        // storage via channels
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(req) = put_rx.recv() => {
                        nkv.put(&req.key, req.value).await;
                        let _ = req.resp_tx.send(nkv::NotifyKeyValueError::NoError).await;
                    }
                    Some(req) = get_rx.recv() => {
                        let vals = nkv.get(&req.key);
                        if vals.len() > 0 {
                            req.resp_tx.send(NkvGetResp {
                                value: vals,
                                err: nkv::NotifyKeyValueError::NoError
                            }).await;
                        } else {
                            req.resp_tx.send(NkvGetResp {
                                value: Vec::new(),
                                err: nkv::NotifyKeyValueError::NotFound
                            }).await;
                        }
                   }
                   Some(req) = del_rx.recv() => {
                       let err = match nkv.delete(&req.key).await {
                           Ok(_) => nkv::NotifyKeyValueError::NoError,
                           Err(e) => e,
                       };
                       let _ = req.resp_tx.send(err).await;
                   }
                   Some(req) = sub_rx.recv() => {
                       let mut err = nkv::NotifyKeyValueError::NoError;
                       match nkv.subscribe(&req.key, req.addr, req.writer).await {
                           Err(e) => { err = nkv::NotifyKeyValueError::NotifierError(e) }
                           Ok(_) => {}
                       }
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
        let listener = TcpListener::bind(self.addr).await.unwrap();
        loop {
            let put_tx = self.put_tx();
            let get_tx = self.get_tx();
            let del_tx = self.del_tx();
            let sub_tx = self.sub_tx();

            tokio::select! {
                Ok((stream, addr)) = listener.accept() => {
                    let (read_half, write_half) = split(stream);
                    let mut reader = BufReader::new(read_half);
                    let writer = BufWriter::new(write_half);

                    tokio::spawn(async move {
                        let mut buffer = String::new();
                        match reader.read_line(&mut buffer).await {
                            Ok(0) => {
                                // Connection was closed
                                return;
                            }
                            Ok(_) => match serde_json::from_str::<ServerRequest>(&buffer.trim()) {
                                Ok(request) => {
                                    match request {
                                        ServerRequest::Put(PutMessage { .. }) => {
                                            Self::handle_put(writer, put_tx.clone(), request).await
                                        }
                                        ServerRequest::Get(BaseMessage { .. }) => {
                                            Self::handle_get(writer, get_tx.clone(), request).await
                                        }
                                        ServerRequest::Delete(BaseMessage { .. }) => {
                                            Self::handle_delete(writer, del_tx.clone(), request).await
                                        }
                                        ServerRequest::Subscribe(BaseMessage { .. }) => {
                                            Self::handle_sub(writer, sub_tx.clone(), request, addr).await
                                        }
                                    };
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse JSON: {}", e);
                                }
                            },
                            Err(_) => {
                                eprintln!("Failed to match request");
                            }
                        }
                    });
                }

                _ = &mut self.cancel_rx => {
                    return;
                }
            }
        }
    }

    async fn write_response(reply: ServerResponse, mut writer: WriteStream) {
        let json_reply = serde_json::to_string(&reply).unwrap();
        if let Err(e) = writer.write_all(&json_reply.into_bytes()).await {
            eprintln!("Failed to write to socket; err = {:?}", e);
            return;
        }
        if let Err(e) = writer.flush().await {
            eprintln!("Failed to flush writer; err = {:?}", e);
        }
    }

    async fn handle_put(
        writer: WriteStream,
        nkv_tx: mpsc::UnboundedSender<PutMsg>,
        req: request_msg::ServerRequest,
    ) {
        match req {
            request_msg::ServerRequest::Put(request_msg::PutMessage { base, value }) => {
                let (resp_tx, mut resp_rx) = mpsc::channel(1);
                // TODO: handle error and throw response
                let _ = nkv_tx.send(PutMsg {
                    key: base.key,
                    value,
                    resp_tx,
                });
                let nkv_resp = resp_rx.recv().await.unwrap();
                let resp = request_msg::BaseResp {
                    id: base.id,
                    status: nkv_resp.to_http_status(),
                    message: nkv_resp.to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
            _ => {
                let resp = request_msg::BaseResp {
                    id: "0".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "wrong message for put handle".to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
        }
    }

    async fn handle_get(
        writer: WriteStream,
        nkv_tx: mpsc::UnboundedSender<GetMsg>,
        req: request_msg::ServerRequest,
    ) {
        match req {
            request_msg::ServerRequest::Get(request_msg::BaseMessage { id, key }) => {
                let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
                let _ = nkv_tx.send(GetMsg {
                    key,
                    resp_tx: get_resp_tx,
                });
                let nkv_resp = get_resp_rx.recv().await.unwrap();
                let data: Vec<Vec<u8>> = nkv_resp
                    .value
                    .into_iter()
                    .map(|arc| arc.as_ref().to_vec())
                    .collect();
                let resp = request_msg::DataResp {
                    base: request_msg::BaseResp {
                        id,
                        status: nkv_resp.err.to_http_status(),
                        message: nkv_resp.err.to_string(),
                    },
                    data,
                };
                Self::write_response(ServerResponse::Get(resp), writer).await;
            }
            _ => {
                let resp = request_msg::BaseResp {
                    id: "0".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "wrong message for get  handle".to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
        }
    }

    async fn handle_delete(
        writer: WriteStream,
        nkv_tx: mpsc::UnboundedSender<BaseMsg>,
        req: request_msg::ServerRequest,
    ) {
        match req {
            request_msg::ServerRequest::Delete(request_msg::BaseMessage { id, key }) => {
                let (resp_tx, mut resp_rx) = mpsc::channel(1);
                let _ = nkv_tx.send(BaseMsg { key, resp_tx });
                let nkv_resp = resp_rx.recv().await.unwrap();
                let resp = request_msg::BaseResp {
                    id,
                    status: nkv_resp.to_http_status(),
                    message: nkv_resp.to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
            _ => {
                let resp = request_msg::BaseResp {
                    id: "0".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "wrong message for the handle".to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
        }
    }

    async fn handle_sub(
        writer: WriteStream,
        nkv_tx: mpsc::UnboundedSender<SubMsg>,
        req: request_msg::ServerRequest,
        addr: SocketAddr,
    ) {
        match req {
            request_msg::ServerRequest::Subscribe(request_msg::BaseMessage { id: _, key }) => {
                let (resp_tx, _) = mpsc::channel(1);
                let _ = nkv_tx.send(SubMsg {
                    key,
                    addr,
                    writer,
                    resp_tx,
                });
            }
            _ => {
                let resp = request_msg::BaseResp {
                    id: "0".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "wrong message for sub handle".to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notifier::Message;
    use crate::NkvClient;
    use tempfile::TempDir;
    use tokio::{self, net::TcpStream};

    #[tokio::test]
    async fn test_server() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = "127.0.0.1:8091";

        let (mut srv, _cancel) = Server::new(url.to_string(), temp_dir.path().to_path_buf())
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
        assert!(matches!(message, nkv::NotifyKeyValueError::NoError));

        let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert!(matches!(got.err, nkv::NotifyKeyValueError::NoError));

        assert_eq!(got.value, vec!(Arc::from(value)));

        // create sub
        let addr: SocketAddr = url.parse().expect("Unable to parse addr");
        let stream = TcpStream::connect(&url).await.unwrap();
        let (_, write) = tokio::io::split(stream);
        let writer = BufWriter::new(write);

        let _ = sub_tx.send(SubMsg {
            key: key.clone(),
            resp_tx: resp_tx.clone(),
            addr,
            writer,
        });
        let got = resp_rx.recv().await.unwrap();
        assert!(matches!(got, nkv::NotifyKeyValueError::NoError));

        let _ = del_tx.send(BaseMsg {
            key: key.clone(),
            resp_tx: resp_tx.clone(),
        });
        let got = resp_rx.recv().await.unwrap();
        assert!(matches!(got, nkv::NotifyKeyValueError::NoError));

        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert!(matches!(got.err, nkv::NotifyKeyValueError::NotFound));
    }

    #[tokio::test]
    async fn test_client_server() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = "127.0.0.1:8092";

        let (mut srv, _cancel) = Server::new(url.to_string(), temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tokio::spawn(async move {
            srv.serve().await;
        });

        // Give time for server to get up
        // TODO: need to create a notification channel
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let mut client = NkvClient::new(&url);

        let value: Box<[u8]> = Box::new([9, 7, 3, 4, 5]);
        let key = "test_2_key1".to_string();

        let resp = client.put(key.clone(), value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
            })
        );

        let get_resp = client.get(key.clone()).await.unwrap();
        assert_eq!(
            get_resp,
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: "0".to_string(),
                    status: http::StatusCode::OK,
                    message: "No Error".to_string(),
                },
                data: vec!(value.to_vec()),
            })
        );

        let err_get_resp = client.get("non-existent-key".to_string()).await.unwrap();
        assert_eq!(
            err_get_resp,
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: "0".to_string(),
                    status: http::StatusCode::NOT_FOUND,
                    message: "Not Found".to_string(),
                },
                data: Vec::new(),
            })
        );

        let (tx, mut rx) = mpsc::channel(1);
        let send_to_channel = Box::new(move |value| {
            let tx = tx.clone();
            tokio::spawn(async move {
                tx.send(value).await.unwrap();
            });
        });

        let sub_resp = client
            .subscribe("non-existent-key".to_string(), send_to_channel.clone())
            .await
            .unwrap();
        assert_eq!(
            sub_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: http::StatusCode::OK,
                message: "Subscribed".to_string(),
            })
        );

        let sub_resp = client
            .subscribe(key.clone(), send_to_channel.clone())
            .await
            .unwrap();
        assert_eq!(
            sub_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: http::StatusCode::OK,
                message: "Subscribed".to_string(),
            })
        );
        // Give server time to subscribe
        // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let new_value: Box<[u8]> = Box::new([42, 0, 1, 0, 1]);
        let resp = client.put(key.clone(), new_value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
            })
        );

        if let Some(Message::Update { key: _, value }) = rx.recv().await {
            assert_eq!(value, new_value);
        } else {
            panic!("Expected value");
        }

        let del_resp = client.delete(key.clone()).await.unwrap();
        assert_eq!(
            del_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: "0".to_string(),
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
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
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: "0".to_string(),
                    status: http::StatusCode::NOT_FOUND,
                    message: "Not Found".to_string(),
                },
                data: Vec::new(),
            })
        );
    }
}
