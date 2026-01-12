# miniraft

An implementation of the [Raft distributed consensus
protocol](https://raft.github.io/raft.pdf) for powering a key-value workload (read,
write, CAS).

- **dependency-free**, stdlib-only Rust (cold, release compile in ~1s)
  - [**custom JSON parser**](json/src/lib.rs) and ser/de framework, with OK performance
    (ballpark of ~220 MB/s throughput on Apple M3 on [complex
    input](json/benches/bench.rs) (UTF-16 surrogate etc.)) and **full spec compliance**,
    passing [JSON minefield stress test
    suite](https://seriot.ch/software/parsing_json.html)
  - [base64 non-URL](base64/src/lib.rs)
  - [`rand` helper](rand/src/lib.rs) (Unix only)
  - [Prometheus-style metrics](raft/src/metrics/mod.rs) with a simple HTTP/1.1 server
- Raft implementation passes [Jepsen Maelstrom chaos
  testing](https://github.com/jepsen-io/maelstrom), for the [**linearizable** KV
  workload](docs/images/raft-kv-latencies-under-network-partition.png) (the failures in
  the graph are expected for linearizability)
    - **fully generic (literally and design-wise) core Raft**: applicable to _any_
      workload backable by Raft; the core just holds opaque commands in its log, with an
      abstract state machine dependency-injected for committing into
    - channel-based glue layer to translate between KV RPCs and Raft core; enables
      pluggable I/O
    - focus on correctness: [illegal states made
      unrepresentable](https://cliffle.com/blog/rust-typestate/) levering the type
      system where feasible, and liberal use of `assert`s for pre/post conditions. No
      `unsafe`, no shenanigans

## Not in the box

- TCP/HTTP: I/O and [the binary](raft/src/main.rs) are specific to Jepsen
  Maelstrom, but that's just a couple hundred lines
- async: stdlib-only, so threading is used.

  This is implemented asynchronously and non-blocking all the same: client and Raft RPCs
  can be emitted and received at any time. Awaiting responses to emitted RPCs is
  asynchronous as well, though messages go into single-receiver queues. That serializes
  requets, but the necessary mutual exclusion on the core Raft state does so anyway
  (there is only one lock, which incidentally makes deadlock-freedom much simpler to
  guarantee!)
- live cluster configuration changes
