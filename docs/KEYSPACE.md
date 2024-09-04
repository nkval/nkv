# Notify Key Value (nkv) keyspace

Notify Key Value provides in-built keyspace functionality with `.` symbol as separator supporting wildcards.
What does that mean? Well, values are stored hierarchically and each level of hierarchy is separated by `.`
And if you want to get all the values stored under certain hierarchy level you can use wildcard, i.e.

```rust
let url = "localhost:4222".to_string();
let mut client = NkvClient::new(&url);

let value: Box<[u8]> = Box::new([9, 7, 3, 4, 5]);

let resp = client.put("service.topicA.key1".to_string(), value.clone()).await.unwrap();
let resp = client.put("service.topicA.key2".to_string(), value.clone()).await.unwrap();
let resp = client.put("service.topicB.key3".to_string(), value.clone()).await.unwrap();
let resp = client.put("service.topicB.key4".to_string(), value.clone()).await.unwrap();


let get_resp = client.get("service.*").await.unwrap();
// service.topicA.key1
// service.topicA.key2
// service.topicB.key3
// service.topicB.key4
print("{:?}", get_resp);

let get_resp = client.get("service.topicA.*").await.unwrap();
// service.topicA.key1
// service.topicA.key2
print("{:?}", get_resp);

let get_resp = client.get("service.topicA.key1").await.unwrap();
// service.topicA.key1
print("{:?}", get_resp);
```
