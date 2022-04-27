# Notes on KV store

## What is the point of key-value stores?

It might seem strange to bother with a seemingly primtive
storage solution in which only simple key/value pairs can
be stored. It's just a hashmap, isn't it?

But like many Redis introductions have pointed out, simple
in-memory KV stores are faster than complex relational
database in some cases. Such cases include, but not limited
to tracking web app sessions, web access analysis and statistics,
progress bars, CSRF tokens, temporary OAuth tokens and etc.

It's truly convenient to throw some handshake data in a key-value
store, and it's fast performance-wise, because "hitting a database
every few seconds is expensive, but hitting Redis every few seconds
(even for thoudsands of concurrent users) is no problem at all... A
relational database is wasted on this kind of short-lived data." [^1]

Despite convenience, the biggest killer feature of Redis is probably
its "screamingly" high performance, and it achieved 200k gets/sets
operations on my M1 Air. And 50% of the requests are completed within
0.135 msec.

[^1]: [Redis tutorial, April 2010 - by Simon Willison](https://static.simonwillison.net/static/2010/redis-tutorial/)


## Design notes

### Master/Replica Communication

#### Handshake

Handshake happens when the master tries to establish connection with the replica,
where the master send an handshake request and the replica ACKed back.

We perform handshakes in two cases:

1. When the master and replicas first start up, the master sends handshake messages
   to all provided replicas' addresses and expect to receive ACKs from everyone.
2. When one of the replicas is down, the master will send handshake request periodically
   (with timeout), waiting for it to come back online.
3. To ensure all replicas are running, we send handshakes as heartbeat messages periodcallly.

#### RESP Packet

When the master reveived an GET/SET/DEL command encoded in RESP format, it choose an
replica and forward the command to it, and later receive the outcome, giving the resulting
RESP packet back to the client.

#### Replicating

When a failed replica come back online, we need to copy existing data to it.
For simplicity, we block all RESP packets for now, forbidding any data changes.
The master then asks a working replica for the full data, forwarding it to the
newly started replica.

#### Shenanigans

Did we say that anybody can fail at anytime? What happens when
the master got killed when replicating data?

#### Packet format

```rust
/// Coordination packet format
pub enum CordValue {
    Handshake,

    // Replica responds `Ack` when getting `Handshake` or `Launch`,
    // `Ack(0)` means the replica just started and have no data,
    // `Ack(other)` means the replica is running, and `other` is
    // the replica ID.
    Ack(u32),

    Resp(Vec<u8>),
    Replicate(HashMap<String, String>),
}
```
