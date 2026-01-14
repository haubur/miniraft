use std::collections::HashMap;
use std::fs::{self};
use std::io::{Cursor, ErrorKind};
use std::path::Path;
use std::sync::mpsc;
use std::thread;

use raft::maelstrom::NodeMessageIDGenerator;
use raft::maelstrom::infra::{read, route_incoming, send};
use raft::maelstrom::rpc::MessageEnvelope;
use raft::persistence::{FileMoF, Persistent};
use raft::rpc::Message;
use raft::{Engine, supervisor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (this_node, peers) = supervisor::gate(std::env::vars_os())?;
    eprintln!("process: passed gate: this node: {this_node}, peers: {peers:?}");

    // See if we have existing durable state from past runs. Start from scratch if we
    // don't. I/O errors outside of the file outright missing are fatal at this stage.
    let (persistence_path, persistent_state) = {
        let path = Path::new("./node").join(&this_node);

        std::fs::create_dir_all(path.parent().expect("has parent"))?;

        eprintln!("persistence: checking path {}", path.to_string_lossy());
        match fs::read_to_string(&path) {
            Ok(s) if s.is_empty() => {
                // Might happen if it was created but never initially written to before
                // crashing.
                eprintln!("persistence: restore: file exists but empty");
                (path, Persistent::default())
            }
            Ok(s) => {
                eprintln!("persistence: restore: reading from file");
                (path, Persistent::restore(&mut Cursor::new(s))?)
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                eprintln!("persistence: restore: file does not exist");
                (path, Persistent::default())
            }
            Err(e) => {
                eprintln!("persistence: restore: unexpected error: {e}");
                return Err(e.into());
            }
        }
    };

    let persistence_file = FileMoF::new(persistence_path)?;

    // Other Raft peers contacting this node (at any time)
    let (raft_incoming_tx, raft_incoming_rx) = mpsc::channel();

    // This node contacting other Raft peers (at any time)
    let (raft_outgoing_tx, raft_outgoing_rx) = mpsc::channel();

    // Cluster clients contacting this node (at any time)
    let (client_incoming_tx, client_incoming_rx) = mpsc::channel();

    // This node contacting cluster clients (at any time)
    let (client_outgoing_tx, client_outgoing_rx) = mpsc::channel();

    let metrics_addr = (
        "127.0.0.1",
        11_000
            + this_node
                .strip_prefix(|c: char| !c.is_numeric())
                .ok_or("need node name with numeric component for stable metrics port")?
                .parse::<u16>()?,
    );

    // Note, the concrete key-value types are set below, **a single time** for the
    // **entire application**. They cascade down everywhere, and are thus easily
    // pluggable (might need to provide ser/de implementations though) at no performance
    // cost (generic, not `dyn`). E.g., could be `<String, String>`.
    let raft: Engine<HashMap<u64, i64>> =
        Engine::new(this_node.clone(), peers.clone(), persistent_state);
    raft.start(
        raft_incoming_rx,
        raft_outgoing_tx,
        client_incoming_rx,
        client_outgoing_tx,
        persistence_file,
        NodeMessageIDGenerator,
        metrics_addr,
    );

    // Handle outgoing Raft messages
    thread::Builder::new()
        .name("handle-outgoing-raft-messages".into())
        .spawn({
            let this_node = this_node.clone();

            move || {
                for (node, msg) in raft_outgoing_rx {
                    if let Err(e) = send(&MessageEnvelope {
                        source: this_node.clone(),
                        destination: node,
                        body: msg,
                    }) {
                        eprintln!("error sending outgoing Raft message: {e}")
                    };
                }
            }
        })
        .expect("thread creation should succeed");

    // Handle outgoing client messages
    thread::Builder::new()
        .name("handle-outgoing-client-messages".into())
        .spawn({
            let this_node = this_node.clone();

            move || {
                for (client, msg) in client_outgoing_rx {
                    if let Err(e) = send(&MessageEnvelope {
                        source: this_node.clone(),
                        destination: client,
                        body: msg,
                    }) {
                        eprintln!("error sending outgoing client message: {e}");
                    };
                }
            }
        })
        .expect("thread creation should succeed");

    // Handle incoming messages (Raft and KV clients).
    let mut buf = String::with_capacity(128);
    let mut n = 0u64;
    loop {
        n += 1;
        let m: MessageEnvelope<Message<_, _, _>> = read(&mut buf)?;
        let raft_incoming_tx = raft_incoming_tx.clone();
        let client_incoming_tx = client_incoming_tx.clone();

        // Handle message.
        thread::Builder::new()
            .name(format!("handle-incoming-msg-{n}"))
            .spawn(|| {
                route_incoming(m, raft_incoming_tx, client_incoming_tx);
            })
            .expect("thread creation should succeed");
    }
}
