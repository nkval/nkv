# How does `nkv` work exactly?

You can think about there are two main components in `nkv`: `Server` and `NkvStorage`.
`Server` gives asynchronous access to clients for `NkvStorage` since latter is made
synchronous by design to simplify the architecture and components. `Server` is responsible
to handle client connections.

`NkvClient` sends requests to `Server` through a connection and `Server` interacts with `NkvStorage`
struct via Rust channels and `NkvStorage` struct interacts then with `StoragePolicy` to store
value on a file system and `Notifier` which sends to `Subscribers` updates whenever value is changed and
send Close message whenever value is deleted. It also stores everything in a Trie structure as a cache to
access elements.

From the flow diagram you can see how `NkvStorage` processes requests.

![nkv flow diagram](../imgs/nkv_flow.drawio.png)

`StoragePolicy` and `Notifier` are traits and `NkvStorage` is a cache mechanism for a generic 
`Value` which is composed of `StoragePolicy` and `Notifier`. So one can define their own `StoragePolicy`
and/or `Notifier`. That is done because default implementation of `nkv` might not be suitable to your 
particular use case, for example you want to store values in a mysql database or you do not want 
to keep them at all, or you want to notify your clients via unix domain socket or zmq socket
because your existing system relies on it or you think it's better, etc. In code it will look something
like this:

```rust
let mut none_tcp_nkv = NotifyKeyValue::<NoStorage, TcpNotifier>::new(path)?;
let mut redis_tcp_nkv = NotifyKeyValue::<RedisStorage, TcpNotifier>::new(path)?;
let mut mysql_uds_nkv = NotifyKeyValue::<MySqlStorage, UnixSockNotifier>::new(path)?;
```

In theory, if you don't like the caching mechanism, `Trie` could be turned into a trait and then you can
implement `LRUTrie` or `NoCache`.

## Default implementaiton 

The way `Server` is handling *subscribe* is different from other API calls: TCP connection is not closed,
but rather kept open to send messages to `Subscriber`. So `NkvClient` when called `subscribe()` creates a 
`Subscriber` struct and stores it, which in turns in its own thread listens to messages comming from `NkvStorage`
 via `Notifier` and newest value would be sent to `tokio::watch` channel.
`NkvStorage` is a map of String Key and a value containing `StoragePolicy` and `Notifier`.
`StoragePolicy` is an object, which stores your state (some variable) on file system, so that
you can restart your application without worrying about data loss. `Notifier` on the other hand
handles the channel between server and client to notify latter if anybody changed value. This 
channel is a OS-primitive (sockets) so you can 
write clients in any programming language you want. Last two components are `Server` and `NkvClient`.
Former creates `NkvStorage` object and manages its access from asynchronous requests. It does 
so by exposing endpoints through some of the OS-primitive (for example, socket), so again, clients could
be in any programming language you like; Latter is used to connect to `Server` and implement the API
