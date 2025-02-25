# Design decisions

This document is a collection of decisions answering **Whys** that developer might encounter
looking at the code. If you think that your patch might raise why which would require a thorough
explanation, please, write it here and refer to this doc in the comment section.

### TcpNotifier

#### Why use buffer to guarantee latest state on consumers?

Send update to multiple clients can be very time-consuming operation, which will lock sending other states, we want to be able to do so in async manner, however, just putting update messages in a separate thread, firing and forgeting about them won't do the trick. Because it so may happen that during updates a new value would come in and then state of the system is undertermined, could be partially updated, could be only old value, could be new value. Having a queue might also lead to infinite queue or DDoS of the system, best way would be a lock-free queue. You can think of buffer of two as a lock free queue, which operates in a following way: when we are updating clients (consumers) with a new value, we lock that value and if any other client wants to write a new value, we do so in another element of our array (assuming that clients do not create a race condition themselves) and since we are Boxing, updating element is a cheap operation.

### Dockerfile

#### Why use musl and link binary statically?

We want docker image to be as small as possible, for that we want to use scratch image.
It is easier to just copy one statically linked binary to a container rather than finding out,
what libs are required and then copyting them there (although, it is doable). As of now, only
musl supports static linking. There are known issues with musl performance described in this
[article](https://andygrove.io/2020/05/why-musl-extremely-slow/) but for now we regard this 
problem as premature optimisation, we need to figure out, which APIs and clients are useful. 

### Logging

#### Why trace library?

nkv is asynchronous system, interpreting traditional log messages can be quite complicated.
trace allows libraries and applications to record structured events with additional information
about *temporality* and *causality* — unlike a log message, a span in tracing has a beginning
and end time, may be entered and exited by the flow of execution, and may exist within a nested
tree of similar spans. In addition, tracing spans are structured, with the ability to record
typed data as well as textual messages. [1](https://docs.rs/tracing/latest/tracing/)

Another important assumption is that nkv would be deployed in a multi-service architecture
where log collection is not trivial, hence we want to give developers flexibility of adding
different sinks to write logs to.

### StorageEngine

### How does FileStorage work?

FileStorage implements StorageEngine and allows to save state on disk main feature of this structure is to allow saving files atomically, so there will be no corrupted files saved.

- First, two directories in the root directory are created: ingest and digest.
- Each file stored is written to ingest path
- Then that file is renamed to the same location in digest path

All of this is done in write_atomic function, since renaming is atomic operation in OS context, there won't be any corrupted files, we create two directories in specified root folder, because renaming could only be done within the _same_ file system.
