use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::mpsc;
use std::{io, thread};

use raft::Engine;
use raft::maelstrom::NodeMessageIDGenerator;
use raft::maelstrom::infra::{read, read_and_handle_init, route_incoming, send};
use raft::maelstrom::rpc::MessageEnvelope;
use raft::persistence::Persistent;
use raft::rpc::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = String::with_capacity(64);

    let (this_node, remote_nodes) = {
        let (this_node, mut remote_nodes) = read_and_handle_init(&mut buf)?;
        remote_nodes.retain(|id| *id != this_node);
        (this_node, remote_nodes) // de-mut
    };

    // See if we have existing durable state from past runs. Start from scratch if we
    // don't. I/O errors outside of the file outright missing are fatal at this stage.
    let (persistence_file, persistent_state) = {
        let path = Path::new("./.state/node").join(&this_node);
        std::fs::create_dir_all(path.parent().expect("has parent"))?;
        match File::create_new(&path) {
            Ok(f) => {
                eprintln!(
                    "persistence: new empty file at {}",
                    path.canonicalize()?.to_string_lossy()
                );
                (f, Default::default())
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                let mut f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)?; // reverse TOCTOU?!
                let p = Persistent::restore(&mut f)?;
                eprintln!(
                    "persistence: restored from {}",
                    path.canonicalize()?.to_string_lossy()
                );
                (f, p)
            }
            Err(e) => return Err(e.into()),
        }
    };

    // Other Raft peers contacting this node (at any time)
    let (raft_incoming_tx, raft_incoming_rx) = mpsc::channel();

    // This node contacting other Raft peers (at any time)
    let (raft_outgoing_tx, raft_outgoing_rx) = mpsc::channel();

    // Cluster clients contacting this node (at any time)
    let (client_incoming_tx, client_incoming_rx) = mpsc::channel();

    // This node contacting cluster clients (at any time)
    let (client_outgoing_tx, client_outgoing_rx) = mpsc::channel();

    // Note, the concrete key-value types are set below, **a single time** for the
    // entire application. They cascade down to everything through generics + type
    // inference, and are thus easily pluggable (might need to provide ser/de
    // implementations!) at no performance cost (everything is generic, not `dyn`).
    let raft: Engine<HashMap<u64, i64>> =
        Engine::new(this_node.clone(), remote_nodes.clone(), persistent_state);
    raft.start(
        raft_incoming_rx,
        raft_outgoing_tx,
        client_incoming_rx,
        client_outgoing_tx,
        persistence_file,
        NodeMessageIDGenerator,
    );

    // Handle outgoing Raft messages
    thread::Builder::new()
        .name("handle-outgoing-raft-messages".into())
        .spawn({
            let this_node = this_node.clone();

            move || {
                for (node, msg) in raft_outgoing_rx {
                    send(&MessageEnvelope {
                        source: this_node.clone(),
                        destination: node,
                        body: msg,
                    });
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
                    send(&MessageEnvelope {
                        source: this_node.clone(),
                        destination: client,
                        body: msg,
                    });
                }
            }
        })
        .expect("thread creation should succeed");

    // Handle incoming messages (Raft and KV clients).
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
