use std::sync::{Arc, Mutex};
use std::thread;

use kv::Store;
use kv::infra::{process, read, respond};
use maelstrom::NodeMessageIdGenerator;
use maelstrom::rpc::{Message, Request, Response};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Global and stable across a node lifecycle
    let mut node_id = None;

    // Got K, V types from testing
    let store = Arc::new(Mutex::new(Store::<u64, i64>::new()));

    // Try and fit into a cache line (best effort)
    let mut buf = String::with_capacity(64);

    loop {
        let m = read(&mut buf)?;

        if node_id.is_none() {
            let Message {
                source,
                body:
                    Request::Init {
                        message_id,
                        node_id: init_node_id,
                        ..
                    },
                ..
            } = m
            else {
                return Err("invalid initial request".into());
            };

            respond(&Message::<Response<'_, ()>> {
                source: init_node_id.clone(),
                destination: source,
                body: Response::InitOk {
                    in_reply_to: message_id,
                },
            });

            node_id = Some(init_node_id);
            continue;
        }

        // Part of the protocol contract; no need to be robust here for local use
        let node_id = node_id
            .as_ref()
            .ok_or("node ID not set on first iteration")?
            .clone();

        // Let's act like this actually increases throughput...
        thread::spawn({
            let store = Arc::clone(&store);
            move || {
                if let Err(e) = process(m, node_id, NodeMessageIdGenerator, store) {
                    // We could panic here but we will either send errors out anyway, or
                    // fail to reply; in any case, failure will be caught.
                    eprintln!("processing failed: {}", e);
                };
            }
        });
    }
}
