use std::sync::mpsc;
use std::thread;

use json::serde::{Deserialize, Serialize};
use raft::Raft;
use raft::maelstrom::NodeMessageIdGenerator;
use raft::maelstrom::infra::{handle_incoming, handle_outgoing, read, read_and_handle_init};
use raft::maelstrom::rpc::MessageEnvelope;
use raft::rpc::Message;
use raft::state::Persistent;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = String::with_capacity(64);

    let (this_node, mut remote_nodes) = read_and_handle_init(&mut buf)?;
    remote_nodes.retain(|id| *id != this_node);

    let p = Persistent::default(); // start from scratch

    let (incoming_tx, incoming_rx) = mpsc::channel();
    let (outgoing_tx, outgoing_rx) = mpsc::channel();

    let raft: Raft<DummyCommand> = Raft::new(this_node.clone(), remote_nodes.clone(), p);
    raft.start(incoming_rx, outgoing_tx);

    // Handle outgoing messages (which are all Raft messages).
    thread::Builder::new()
        .name("raft-outgoing-msgs".into())
        .spawn({
            let remote_nodes = remote_nodes.clone();
            let this_node = this_node.clone();

            move || {
                for msg in outgoing_rx {
                    handle_outgoing(this_node.clone(), &remote_nodes, msg);
                }
            }
        })
        .expect("thread creation should succeed");

    // Handle incoming messages (Raft and KV clients).
    let mut n = 0;
    loop {
        n += 1;
        let m: MessageEnvelope<Message<u64, i64, DummyCommand>> = read(&mut buf)?;
        let node_id = this_node.clone();
        let incoming_tx = incoming_tx.clone();

        // Handle message.
        thread::Builder::new()
            .name(format!("handle-incoming-msg-{n}"))
            .spawn({
                move || {
                    if let Err(e) = handle_incoming(m, incoming_tx, node_id, NodeMessageIdGenerator)
                    {
                        // We could panic here but we will either send errors out anyway, or
                        // fail to reply; in any case, failure will be caught.
                        eprintln!("processing failed: {}", e);
                    };
                }
            })
            .expect("thread creation should succeed");
    }
}

/// TODO: make a fully-feature command suitable for Raft log
#[derive(Debug, Clone, Default)]
struct DummyCommand;

impl Serialize for DummyCommand {
    fn serialize(&self) -> Result<json::Value, json::serde::SerializeError> {
        unimplemented!("not hit yet")
    }
}

impl Deserialize for DummyCommand {
    fn deserialize(_: json::Value) -> Result<Self, json::serde::DeserializeError> {
        unimplemented!("not hit yet")
    }
}
