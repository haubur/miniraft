use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use raft::Engine;
use raft::maelstrom::NodeMessageIdGenerator;
use raft::maelstrom::infra::{read, read_and_handle_init, route_incoming, send};
use raft::maelstrom::rpc::MessageEnvelope;
use raft::rpc::Message;
use raft::state::Persistent;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = String::with_capacity(64);

    let (this_node, mut remote_nodes) = read_and_handle_init(&mut buf)?;
    remote_nodes.retain(|id| *id != this_node);

    // TODO: do not start from scratch on node boot, actually read this off persisted
    // disk version. Note the command is a "neutral element" of reading a non-existent
    // key, and responding to no client about it.
    let p_bytes =
        br#"{"current_term": 0, "voted_for": null, "log": [{"c": {"t": "r", "k": 0}, "t": 0}]}"#;
    let p = Persistent::restore(&mut p_bytes.as_slice())?;

    let (raft_incoming_tx, raft_incoming_rx) = mpsc::channel();
    let (raft_outgoing_tx, raft_outgoing_rx) = mpsc::channel();

    let (client_incoming_tx, client_incoming_rx) = mpsc::channel();
    let (client_outgoing_tx, client_outgoing_rx) = mpsc::channel();

    let raft: Engine<HashMap<u64, i64>> = Engine::new(this_node.clone(), remote_nodes.clone(), p);
    raft.start(
        raft_incoming_rx,
        raft_outgoing_tx,
        client_incoming_rx,
        client_outgoing_tx,
        NodeMessageIdGenerator,
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
    let mut n = 0;
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
