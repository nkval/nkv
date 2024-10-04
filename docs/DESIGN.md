# How does `nkv` work exactly?

You can think about there are two main components in `nkv` `Server` and `NotifyKeyValue`.
`Server` gives asynchronous access to clients for `NotifyKeyValue` since latter is made
synchronous by design to simplify the architecture and components. `Server` is responsible
to handle client connections.
`NkvClient` sends requests to `Server` through a connection and `Server` interacts with `NotifyKeyValue`
struct via Rust channels and `NotifyKeyValue` struct (or `Nkv`) interacts then with `PersistValue` to store
value on a file system and `Notifier` which sends to `Subscribers` updates whenever value is changed and
send Close message whenever value is deleted.
The way `Server` is handling *subscribe* is different from other API calls: TCP connection is not closed,
but rather kept open to send messages to `Subscriber`. So `NkvClient` when called `subscribe()` creates a 
`Subscriber` struct and stores it, which in turns in its own thread listens to messages comming from `Nkv`
 via `Notifier` and newest value would be sent to `tokio::watch` channel.

`NotifyKeyValue` is a map of String Key and a value containing `PersistValue` and `Notifier`.
`PersistValue` is an object, which stores your state (some variable) on file system, so that
you can restart your application without worrying about data loss. `Notifier` on the other hand
handles the channel between server and client to notify latter if anybody changed value. This 
channel is a OS-primitive (sockets) so you can 
write clients in any programming language you want. Last two components are `Server` and `NkvClient`.
Former creates `NotifyKeyValue` object and manages its access from asynchronous requests. It does 
so by exposing endpoints through some of the OS-primitive (for example, socket), so again, clients could
be in any programming language you like; Latter is used to connect to `Server` and implement the API

From the flow diagram you can see how `NotifyKeyValue` processes requests.

![nkv flow diagram](../imgs/nkv_flow.drawio.png)

# How can I tailor `nkv` to my specific needs?

Default implementation of `nkv` might not be suitable to your particular use case, f.e. you want to store
values in a mysql database or you do not want to keep them at all, or you want to notify your clients via 
unix domain socket or zmq socket because your existing system relies on it or you think it's better, etc.

Well, you can fairly easily do that. We abstract `PersistValue` and `Notifier` as traits. You can see it in
[traits.rs](../src/traits.rs) file. `NotifyKeyValue` structure is using generics to abstract from concrete `PersistValue` and
`Notifier` implementation so writing your own structure which fullfills certain trait you'll be able to create
your version of `nkv`. 
*Note:*

- Implementing different interface you might create or loose some of the constraints that were there. See [descision](./DESIGN_DECISIONS.md) doc for that. 
- Currenltly `Server` and `NotifyKeyValue` are not abstracted through traits. We should try to keep balance between abstracting and keeping things simple.
