/// A binary to run the Raft engine while communication happens via tcp/http instead of stdout (maelstrom).
///
/// The core is the same as in main.rs.
/// This variant strips the crash bombs processes used in main.rs to simulate crashes and reboots for maelstrom.
/// Here, one process spawns N children that run as Raft nodes. The main process just waits for it's child processes.
use raft::Engine;
use raft::http::{ConnectionLimiter, handle_connection, send_tcp};
use raft::maelstrom::NodeMessageIDGenerator;
use raft::maelstrom::rpc::MessageEnvelope;
use raft::persistence::{FileMoF, Persistent};
use std::collections::HashMap;
use std::env;
use std::fs::{self};
use std::io::{Cursor, ErrorKind};
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::vec::Vec;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Child branch: A process that runs Raft.
    if env::var("IAM").is_ok() {
        // Tcp port of this node == node name
        let this_node = env::var("IAM").expect("should have port");
        let port: u16 = this_node.parse().expect("port should be parsable to u16");
        let peers: Vec<String> = env::var("PEERS")
            .expect("should have peers")
            .split(",")
            .map(|s| s.to_string())
            .collect();

        // ---
        // Setup exactly as in raft/src/main.rs
        // ---

        // Only child processes go here:
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

        let metrics_addr = ("127.0.0.1", 11_000 + this_node.parse::<u16>()?);

        // Note, the concrete key-value types are set below, **a single time** for the
        // **entire application**. They cascade down everywhere, and are thus easily
        // pluggable (might need to provide ser/de implementations though) at no performance
        // cost (generic, not `dyn`). E.g., could be `<String, String>`.
        let raft: Engine<HashMap<String, String>> =
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
                        if let Err(e) = send_tcp(&MessageEnvelope {
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
                        if let Err(e) = send_tcp(&MessageEnvelope {
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

        // ---
        // End copying setup from raft/src/main.rs
        // ---

        // Http client requests landing on this nodes TcpStream wait for responses, that
        // can occur on any other process.
        // Do not block on wait, but cache waiting TcpStream and answer it, once the
        // reponse lands on a fresh connection.
        let correlation_map: Arc<Mutex<HashMap<String, TcpStream>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Handling incoming messages is different to main.rs which uses maelstrom.
        // Instead of reading from stdout each node listens on a port for tcp.
        // Via tcp we receive both ClientMessages and RaftMessages.
        // In handle_connection an incoming TcpStream is sniffed and dispatched to handle either of the type.
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let mut pool = ConnectionLimiter::new(16);
        for stream in listener.incoming() {
            let stream = stream.expect("should have a stream");
            let this_node = this_node.clone();
            let mut correlation_map = Arc::clone(&correlation_map);
            let client_incoming_tx = client_incoming_tx.clone();
            let raft_incoming_tx = raft_incoming_tx.clone();
            pool.spawn(move || {
                handle_connection(
                    this_node,
                    stream,
                    &mut correlation_map,
                    client_incoming_tx,
                    raft_incoming_tx,
                )
            });
        }
    }

    // Supervisor branch: A process, that (re-)spawns Raft nodes (children).
    let env_nodes = env::var("NODES").expect("NODES should be available in ENV.");
    let nodes: Vec<&str> = env_nodes.split(",").collect();

    let exe = env::current_exe()?;
    let mut childs = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let mut peers = nodes.clone();
        peers.retain(|&x| x != *node);
        println!("{:?}", peers);
        let child = Command::new(&exe)
            .env("IAM", node)
            .env("PEERS", peers.join(","))
            .spawn()?;
        childs.push(child);
    }

    for mut c in childs {
        if let Err(e) = c.wait() {
            eprintln!("error waiting for child {e}")
        };
    }
    Ok(())
}
