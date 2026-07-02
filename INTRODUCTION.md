**What, Why, How**

# Learning Raft with Alex Povel's miniraft
There is already a lot of material out there to study and understand consensus algorithms in general and Raft in particular.
In fact the one of the reasons why Raft exists is for educational purposes.
When I set out to go hands on with Raft, I recalled that I have seen a talk by Alex Povel at a Rust Meetup, who introduced Raft using his educational implementation **miniraft**.
**miniraft** is very concise, clean and dependency free making it the perfect candidate to study the core of Raft without digging through a bunch of boilerplate and abstractions.
Note, that it demonstrates the essence of the Raft algorithm and is not to be misunderstood as a full-featured consensus engine.
Instead the communication protocol is piped to maelstrom, which itself is a testing bed when building distributed systems. More on that later.

So what we will do here is three things.
1. Discuss the core of Raft and how it can be implemented in Rust.
3. Brief discussion of maelstrom and how it is used in miniraft.
3. Implement I/O via http.

# A brief primer on consensus algorithms and Raft in particular
- What is consensus?
- Why do we need it?
- How can we approach it?

To set the scene assume you are in need of a log like database where data records have a specific order in which the need to be stored, you want to make concurrent reads and writes on your data
and you want to have it replicated such that you do not lose it when a server crashes.

"Raft is a consensus algorithm for managing a replicated log." -paper

The problem of distributed environments where multiple servers need to exchange information for a system or application to do its thing is
that messages can be lost due to network partitions, servers might be temporarily unreachable due to outages, planned maintenance or other reasons.
Additionally, if messages from different servers need to have a causal order, network latency might interfere with that causal order when
a message A is delayed to the point that it arrives after message B even though from a logical perspective it has to be processed before message B.

## Raft implementation in Rust based on Alex Povel's miniraft


### A data records journey through miniraft
To study the Raft algorithm and Alex Povel's neat implementation in his Rust based miniraft, we will follow a data record through it's journey from the
client to a persistet state in a log type database. On the way I try to revist and explain relevant concepts of the algorithm
while leveraging miniraft to illustrate how it can be implemented.

Let's start with the what is what and an outline of the repository. We are looking at commit **FILL** here.
In his README he states that miniraft is dependency free. And as you can see from the Cargo.toml this is true.
Everything needed from that could be a dependency is reimplemented in a minimalistic, very readable way at the cost of optimization.
This includes rand, base64 and json.
**Explain what they are needed for briefly here**.

#### Meeting our data record and the log
So the first question should be what is the *log* to be replicated?
We know it's supposed to be some sort of data stored on disk.

From raft/src/state.rs l.115 ff. we know that a data record is a *LogEntry* which has two fields: (1) *Term* and (2) *Cmd* and
the log is just a bunch of LogEntries stored in a *Vec*. *Cmd* is the actual data and generic in miniraft's implementation. It thus does not
expect a specific data type. *Cmd* is short for command and a naming convention in the literature for state-machine replication. But we can think of it here as the data.
The only thing that Cmd needs to ensure (regardless of what data type is actually realized) is that the *Dismiss* trait is implemented on Cmd.
**What is Dismiss used for? Point to the actual lib.rs implemenation as an example.**
Note, that the log is never flushed to solid-state memory. The data is never persisted to disk, but kept in memory.

To avoid single point of failures and losing all our data we want the log to be replicated. We spin up many machines (aka nodes) on which we want to see the exact same log.
Now, if we want to write a new data record to our log. What should we do? Sent the data record from the client to all nodes that persist the log?

# State machine and server state
To define a state we need a couple of things, some are *common* across all roles and some are unique to the individual roles of *followers*, *candidates* and *leaders*.
The **Common** state is defined in raft/src/state.rs l.211. One is the *Log* which we already discussed. Another is the *Term*. Which is technically a new-type u64.
*Term* is a logical time period tracker for each node. Note that the time period can differ from node to node and when it progresses is defined by the algorithms.
The naming *Term* lends itself from politics, where elected leaders serve a specific period of time called *term*.
Similar to politics all nodes in the system running a replication of the same log must decide on who should be the leader.
But wait, why would we need a leader anyways?
Luckily, we already have discussed enough concepts to derive a proper rationale.
The concept of logical time at the node level is introduced to spin it forwards individually when events happen on the and to determine what node has seen the most recent events.

## Why a leader and how is it elected
## How a client's message reaches the leader
In Raft algorithm, the leader needs to handle all requests from clients. But how do we know who the current leader is?
There are different approaches to hit the leader: refiring the request until you found the leader, having an external server that is leader aware from which you receive the leaders address before
sending the request, but in miniraft a message intended for the leader is sent to any node, regardless of whether its the leader or not is.
Instead, the messages are proxied by the receiving nodes to the leader comp. raft/src/lib.rs:365. This proxy setup is a single hop, because any AppendEntry (either heartbeat or log replication) also returns
the current leader info: state.rs:689-694.

## miniraft and maelstrom

# Implementing http for miniraft
As we have seen in the previous section miniraft uses maelstrom to simulate real world environments for distributed systems, where nodes temporarily crash or are unreachable, to
analyze the effectiveness and correctness of Raft. Consequently, writing and reading from the log is implemented using the maelstrom protcol.
Since it's an interesting excercise and allows broader experimentation with miniraft we will now turn to implementing http to read and write data to the log.

## What we already have
- What do we need to implement http
- What is already there
- Gap analysis: What needs to be built

# A final note on concurrency

# Resources
- [Martin Kleppmann's Distributed Systems Lecture Notes](https://www.cl.cam.ac.uk/teaching/2122/ConcDisSys/dist-sys-notes.pdf)
- [Raft Website](https://raft.github.io/)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [Diego Ongaro's dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [async-raft](https://github.com/async-raft/async-raft)
- [TLA+ specification](https://github.com/ongardie/raft.tla/blob/master/raft.tla)
- [Jon Gejengset's Students Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)

# Notes
- serialization/ parsing (base64/ JSON)
- scheduling tasks
- How is load distributed to replicas?
- Why do we need a leader?
**state-machine** replication is about **total-order broadcast** since
to have multiple instances of the same state machine, all commands need to be in the same order on all machines.
So can we just use total-order broadcast for all nodes? The challenge with total-order is that nodes need to coordinate to
ensure they have the same order in their log. Imagine a case of two nodes A and B each receiving some update from a client.
They need to write that update to their own log and ensure that the respective other node also writes the update to its log.
- **every new message spawns a new thread: raft/src/main.rs:131-144**
  - this is not free and one would not do it in production settings, instead
  - fixed-thread pool: N worker threads pull from a queue
  - thread-per-core + async
- miniraft implements "thread-per-task with blocking I/O"


# Implementing http for miniraft
- Implementing http is taking a TcpStream, expecting data send via that stream has http protocol format and handling that.
- Asynchronous handling of http requests with ThreadPools, e.g. following the [RustBook](https://doc.rust-lang.org/book/ch21-00-final-project-a-web-server.html). Alternatively, use tokio.
- **Question: How to forward requests to Raft nodes?**
