# How do I use it in my Rust project?

To use the client library in your Rust project:

```rust
let url = "localhost:4222".to_string();
let mut client = NkvClient::new(&url);

let value: Box<[u8]> = Box::new([9, 7, 3, 4, 5]);
let key = "test_2_key1".to_string();

let resp = client.put(key.clone(), value.clone()).await.unwrap();
print!("{:?}", resp);
// status: 200
// message: "No Error"

let get_resp = client.get(key.clone()).await.unwrap();
print!("{:?}", resp);
// status: 200
// message: "No Error"
// data: [9, 7, 3, 4, 5]

let sub_resp = client.subscribe(key.clone()).await.unwrap();
print!("{:?}", sub_resp);
// status: 200
// message: "Subscribed"


_ = client.delete(key.clone()).await.unwrap();
```

To use the server in your rust project:

```rust
let temp_dir = TempDir::new().expect("Failed to create temporary directory");
let url = "127.0.0.1:8092";

let srv = Server::new(url.to_string(), temp_dir.path().to_path_buf())
    .await
    .unwrap();
tokio::spawn(async move {
    srv.serve().await;
});
```

### In the server, can I use it between threads inside one program?

Yep, you can use channels to communicate with server

```rust
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

let _ = put_tx.send(PutMsg{key: key.clone(), value: value.clone(), resp_tx: resp_tx.clone()});

let message = resp_rx.recv().await.unwrap();
// nkv::NotifyKeyValueError::NoError
print!("{:?}", message);

let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
let _ = get_tx.send(GetMsg{key: key.clone(), resp_tx: get_resp_tx.clone()});
let got = get_resp_rx.recv().await.unwrap();
// [1, 2, 3, 4, 5] 
print!("{:?}", got.value.unwrap());

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
let message = resp_rx.recv().await.unwrap();

// nkv::NotifyKeyValueError::NoError
print!("{:?}", message);

let _ = del_tx.send(BaseMsg{key: key.clone(), resp_tx: resp_tx.clone()});
let got = resp_rx.recv().await.unwrap();
// nkv::NotifyKeyValueError::NoError
print!("{:?}", got);
```
