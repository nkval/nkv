# How does clients communicate with `nkv` server?

#### Request structure

When using network over the socket, server accepts command in with following pattern

```
+--------------+-----------------+----------------+----------+-------------+
| COMMAND: str | REQUEST-ID: str | CLIENT-ID: str | KEY: str | (DATA): str |
+--------------+-----------------+----------------+----------+-------------+
```

*Note:* if you are using nkv to communicate between threads in your Rust application,
you will use channels to do so. Checkout NkvCore [source](../src/nkv.rs) for more information.

*Important:* requests are valid UTF-8 strings and are delimited by new line

COMMAND, REQUEST-ID, CLIENT-ID and KEY are valid UTF-8 characters, separated by space.
That said all four of them **can't contain space symbols**, KEY supports [keyspace](./KEYSPACE.md)

CLIENT-ID is a convinient way to log requests from multiple clients in one place. Granted, REQUEST-ID
is unique, but with CLIENT-ID it's easier to sort through request coming from a particular client.

Supported commands are: PUT, GET, DEL, SUB, UNSUB, TRACE, HEALTH, VERSION
For more information commands see [this](../README.md) section "What is it for"

DATA section is used only in PUT command. It is base64-encoded string

REQUEST-ID for SUB command is also used to terminate that subscription via UNSUB command.

For client implementation reference in Rust see [this](../src/lib.rs) struct NkvClient.

#### Requests in detail

##### **1. PUT**

**Syntax:**

`PUT REQUEST-ID CLIENT-ID KEY DATA`

**Description:**
Sets the value for a given `KEY` to `DATA`.
- **DATA** is a base64-encoded string.
- **CLIENT-ID** is used on the server side to log requests.

##### **2. GET**

**Syntax:**

`GET REQUEST-ID CLIENT-ID KEY`

**Description:**
Retrieves the stored value for the given `KEY`.
- Returns the result as a **key-value pair**, which allows differentiating between values when using [keyspaces](./KEYSPACE.md).

##### **3. DEL**

**Syntax:**

`DEL REQUEST-ID CLIENT-ID KEY`

**Description:**

Deletes the stored value for the given `KEY`.

##### **4. SUB**

**Syntax:**

`SUB REQUEST-ID CLIENT-ID KEY`

**Description:**
Subscribes the client to receive updates for the given `KEY`.
- Supports [keyspaces](./KEYSPACE.md).
- Updates are sent to the **same socket** (implementation may vary depending on the transport layer).

##### **5. UNSUB**

**Syntax:**

`UNSUB REQUEST-ID CLIENT-ID KEY`

**Description:**
Unsubscribes the client from receiving updates for the given `KEY`.

##### **6. TRACE**

**Syntax:**
`TRACE REQUEST-ID CLIENT-ID KEY`

**Description:**
Returns a list of all `CLIENT-IDs` subscribed to the given `KEY` or any key above it in the hierarchy.


**Example key hierarchy:**

`keyspace1.keyspace2.keyspace3.key`

- If `client-1` is subscribed to `keyspace1` and `client-2` is subscribed to `keyspace3`:
    - `TRACE key` → returns `client-1` and `client-2`
    - `TRACE keyspace2` → returns `client-1`

##### **7. HEALTH**

**Syntax:**

`HEALTH REQUEST-ID CLIENT-ID KEY`

**Description:**
Checks server health status.
- Always returns `OK` if the server is healthy.


##### **8. VERSION**

**Syntax:**

`VERSION REQUEST-ID CLIENT-ID KEY`

**Description:**
Returns the server version as specified in `cargo`.

#### Example of reqests:

Below are examples of requests in string representation, without their length

```
PUT request-id-1 client-id-1 FOO BAR

GET request-id-2 client-id-1 FOO

SUB request-id-3 client-id-1 FOO

UNSUB request-id-3 client-id-1 FOO

DEL request-id-4 client-id-1 FOO
```

#### Response structure:
Responses follow simple structure as well

```
(base|data|trace|version) REQUEST-ID (OK|FAILED) (DATA)
```

DATA are space separated base46-encoded optional strings


#### Notifications structure:

Main feature of nkv is sending notifications when the value is changed. Idea is to keep the same channel you use to communicate with
server to send communications through it (note that nkv is built in a way, where you can choose communication channel, unix socket, tcp
socket, channels, etc. but you might need to implement interfaces to do so). For more information how it all works under the hood check
out [this](./DESIGN.md) doc. For the scope of this document we focus purely on protocol you need to communicate with nkv. So the messages
that you recieve from notifications following this structure:

```
(HELLO|UPDATE|CLOSE|NOTFOUND) KEY DATA
```

First literal is message type: HELLO and NOTFOUND end on that.
They indicate established connection or notifying client that there is no such subscription.

CLOSE message is followed by KEY, which tells the client that certain notifier has been closed.
UPDATE one contains KEY and DATA, latter is base64-encoded value
