# How does `nkv` work exactly?

`nkv` is 3 components:

- core library that provides key-value storage and subscription management, implementing all of the above core functionality, communicated via rust channels, usable by rust programs
- standalone server that wraps the core library to provide a socket-based API accessible to any client that communicates with the API over the socket
- standalone client that simplifies communicating with the server over the socket

The `NkvCore` library allows you to put, get, delete values and subscribe to recieve notifications on value updates via Rust channels.
When instantiated, it receives a parameter called `StorageEngine`. This is what stores the actual data. `StorageEngine` is a trait,
defined [here](../src/traits.rs), and can be implemented via drivers to different storage engines, for example, memory, persistent on disk,
mysql database, or anything else that can implement get, put and delete.

Note that since it is a generic, one instance of `NkvCore` can use *only one `StorageEngine`*. So you cannot store some values in files,
ignore others and write some to sql database.

Yes, for inter-thread communication you can use `nkv` only for Rust language, since it's written in Rust, if you want to connect
applications, written in other programming language together, you will need to run separate `Server` to achieve that. It uses NkvCore structure
and provides communication interface via some OS-primitive like Unix Domain Socket, TCP socket, anything *programming language agnostic*.
Current `Server` implementation is using TCP socket and can be found [here](../src/srv.rs). It is also using a language agnostic protocol to send messages.
They are marshaled in JSON and send via TCP socket, message format could be found [here](../src/srv.rs). `Server` implements the same 5 endpoints
defined above and it uses TCP sockets to handle all the 5 endpoints. In case of subscription it passes the TCP connection to a structure called `Notifier`,
which is nothing more, but a translator from a channel notifications recieved by `Notifier` from `NkvCore` are forwarded to client via TCP connection Notifier owns.

You can see flow diagram for put, get, subscribe below:

![nkv flow diagram](../imgs/nkv_flow.drawio.png)
