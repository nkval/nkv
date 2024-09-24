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
so by exposing endpoints through some of the OS-primitive (socket), so again, clients could
be in any programming language you like; Latter is used to connect to `Server` and implement the API
(see what is it for section)

You can call those components interfaces, and implement them differently to tailor to your particular 
case, i.e. use mysql for persist value or unix domain socket for `Notfifier` and TCP for `Server` 

From the flow diagram you can see how `NotifyKeyValue` processes requests.

![nkv flow diagram](../imgs/nkv_flow.drawio.png)

