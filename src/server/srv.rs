use http::StatusCode;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{split, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use ::nkv::nkv;
use ::nkv::notifier::WriteStream;
use ::nkv::request_msg::{self, BaseMessage, PutMessage, ServerRequest, ServerResponse};

pub struct PutMsg {
    key: String,
    value: Box<[u8]>,
    resp_tx: mpsc::Sender<nkv::NotifyKeyValueError>,
}

pub struct NkvGetResp {
    err: nkv::NotifyKeyValueError,
    value: Option<Arc<[u8]>>,
}

pub struct GetMsg {
    key: String,
    resp_tx: mpsc::Sender<NkvGetResp>,
}

pub struct BaseMsg {
    key: String,
    resp_tx: mpsc::Sender<nkv::NotifyKeyValueError>,
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
}

impl Server {
    pub async fn new(addr: String, path: std::path::PathBuf) -> Result<Self, async_nats::Error> {
        let (put_tx, mut put_rx) = mpsc::unbounded_channel::<PutMsg>();
        let (get_tx, mut get_rx) = mpsc::unbounded_channel::<GetMsg>();
        let (del_tx, mut del_rx) = mpsc::unbounded_channel::<BaseMsg>();
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<SubMsg>();
        let (_cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<BaseMsg>();

        let mut nkv = nkv::NotifyKeyValue::new(path);
        let addr: SocketAddr = addr.parse().expect("Unable to parse addr");

        let srv = Self {
            addr,
            put_tx,
            get_tx,
            del_tx,
            sub_tx,
        };

        // Spawn task to handle Asynchronous access to notify key value
        // storage via channels
        tokio::spawn(async move {
            let mut cancelled = false;
            while !cancelled {
                tokio::select! {
                    Some(req) = put_rx.recv() => {
                        nkv.put(&req.key, req.value).await;
                        let _ = req.resp_tx.send(nkv::NotifyKeyValueError::NoError).await;
                    }
                    Some(req) = get_rx.recv() => {
                        let _ = match nkv.get(&req.key) {
                            Some(resp) => req.resp_tx.send(NkvGetResp {
                                value: Some(resp),
                                err: nkv::NotifyKeyValueError::NoError
                            }).await,
                            None => req.resp_tx.send(NkvGetResp {
                                value: None,
                                err: nkv::NotifyKeyValueError::NotFound
                            }).await
                        };
                   }
                   Some(req) = del_rx.recv() => {
                       nkv.delete(&req.key);
                       let _ = req.resp_tx.send(nkv::NotifyKeyValueError::NoError).await;
                   }
                   Some(req) = sub_rx.recv() => {
                       nkv.subscribe(&req.key, req.addr, req.writer).await;
                       let _ = req.resp_tx.send(nkv::NotifyKeyValueError::NoError).await;
                   }
                   Some(_) = cancel_rx.recv() => { cancelled = true }
                   else => { break; }
                }
            }
        });

        Ok(srv)
    }

    pub async fn serve(&self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();
        loop {
            let put_tx = self.put_tx();
            let get_tx = self.get_tx();
            let del_tx = self.del_tx();
            let sub_tx = self.sub_tx();

            let (stream, addr) = listener.accept().await.unwrap();
            let (read_half, write_half) = split(stream);
            let mut reader = BufReader::new(read_half);
            let writer = BufWriter::new(write_half);

            tokio::spawn(async move {
                let mut buffer = String::new();
                // WARN: could be a bug place because stream is moved
                // and we are using it to read stuff
                //buffer.clear();
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
                                    Self::handle_basic_msg(writer, del_tx.clone(), request).await
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
                    id: 0,
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
                let mut data: Vec<u8> = Vec::new();
                if let Some(v) = nkv_resp.value {
                    data = v.to_vec();
                }
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
                    id: 0,
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "wrong message for get  handle".to_string(),
                };
                Self::write_response(ServerResponse::Base(resp), writer).await;
            }
        }
    }

    async fn handle_basic_msg(
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
                    id: 0,
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
            request_msg::ServerRequest::Subscribe(request_msg::BaseMessage { id, key }) => {
                let (resp_tx, _) = mpsc::channel(1);
                let _ = nkv_tx.send(SubMsg {
                    key,
                    addr,
                    writer,
                    resp_tx,
                });
                // let nkv_resp = resp_rx.recv().await.unwrap();
                // let resp = request_msg::BaseResp {
                //     id,
                //     status: nkv_resp.to_http_status(),
                //     message: nkv_resp.to_string(),
                // };
                // Self::write_response(ServerResponse::Base(resp), writer).await;
            }
            _ => {
                let resp = request_msg::BaseResp {
                    id: 0,
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
    use ::nkv::notifier::Message;
    use ::nkv::NatsClient;
    use tempfile::TempDir;
    use tokio;

    #[tokio::test]
    async fn test_server() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = "127.0.0.1:8091";

        let srv = Server::new(url.to_string(), temp_dir.path().to_path_buf())
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
        assert_eq!(message, nkv::NotifyKeyValueError::NoError);

        let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert_eq!(got.err, nkv::NotifyKeyValueError::NoError);
        assert_eq!(got.value.unwrap(), value.into());

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
        assert_eq!(got, nkv::NotifyKeyValueError::NoError);

        let _ = del_tx.send(BaseMsg {
            key: key.clone(),
            resp_tx: resp_tx.clone(),
        });
        let got = resp_rx.recv().await.unwrap();
        assert_eq!(got, nkv::NotifyKeyValueError::NoError);

        let _ = get_tx.send(GetMsg {
            key: key.clone(),
            resp_tx: get_resp_tx.clone(),
        });
        let got = get_resp_rx.recv().await.unwrap();
        assert_eq!(got.err, nkv::NotifyKeyValueError::NotFound);
    }

    #[tokio::test]
    async fn test_client_server() {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let url = "127.0.0.1:8092";

        let srv = Server::new(url.to_string(), temp_dir.path().to_path_buf())
            .await
            .unwrap();
        tokio::spawn(async move {
            srv.serve().await;
        });

        // Give time for server to get up
        // TODO: need to create a notification channel
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let mut client = NatsClient::new(&url);

        let value: Box<[u8]> = Box::new([9, 7, 3, 4, 5]);
        let key = "test_2_key1".to_string();

        let resp = client.put(key.clone(), value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: 0,
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
            })
        );

        let get_resp = client.get(key.clone()).await.unwrap();
        assert_eq!(
            get_resp,
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: 0,
                    status: http::StatusCode::OK,
                    message: "No Error".to_string(),
                },
                data: value.to_vec(),
            })
        );

        let err_get_resp = client.get("non-existent-key".to_string()).await.unwrap();
        assert_eq!(
            err_get_resp,
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: 0,
                    status: http::StatusCode::NOT_FOUND,
                    message: "Not Found".to_string(),
                },
                data: Vec::new(),
            })
        );

        let sub_resp = client.subscribe(key.clone()).await.unwrap();
        assert_eq!(
            sub_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: 0,
                status: http::StatusCode::OK,
                message: "OK".to_string(),
            })
        );
        // Give server time to subscribe
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let new_value: Box<[u8]> = Box::new([42, 0, 1, 0, 1]);
        let resp = client.put(key.clone(), new_value.clone()).await.unwrap();
        assert_eq!(
            resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: 0,
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
            })
        );
        assert_eq!(true, client.rx.as_mut().unwrap().changed().await.is_ok());
        let msg = client.rx.as_mut().unwrap().borrow().clone();
        assert_eq!(
            msg,
            Message::Update {
                value: new_value.clone(),
            },
        );

        let del_resp = client.delete(key.clone()).await.unwrap();
        assert_eq!(
            del_resp,
            request_msg::ServerResponse::Base(request_msg::BaseResp {
                id: 0,
                status: http::StatusCode::OK,
                message: "No Error".to_string(),
            })
        );
        let del_get_resp = client.get(key.clone()).await.unwrap();
        assert_eq!(
            del_get_resp,
            request_msg::ServerResponse::Get(request_msg::DataResp {
                base: request_msg::BaseResp {
                    id: 0,
                    status: http::StatusCode::NOT_FOUND,
                    message: "Not Found".to_string(),
                },
                data: Vec::new(),
            })
        );
    }
}