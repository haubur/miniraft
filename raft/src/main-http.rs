/// A program that simulates Raft consensus using tcp/http.
///
///
use std::collections::HashMap;
use std::fs::{self};
use std::io::{Cursor, ErrorKind};
use std::path::Path;
use std::sync::mpsc;
use std::thread;

use raft::http::handle_connection;
use raft::maelstrom::NodeMessageIDGenerator;
use raft::maelstrom::infra::{read, route_incoming, send};
use raft::maelstrom::rpc::MessageEnvelope;
use raft::persistence::{FileMoF, Persistent};
use raft::rpc::Message;
use raft::{Engine, supervisor};
use std::env;
use std::net::TcpListener;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Child branch: A process that runs Raft.
    if let Ok(me) = env::var("IAM") {
        // NOTE: TCP port of node is also node name
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
                        if let Err(e) = send(&MessageEnvelope {
                            // TODO: replace with send_tcp: Sends a MessageEnvelope over TCP to some peer.
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
                            // TODO: replace with send_http: Sends a HTTP response in answer to a previous HTTP request.
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

        let correlation_map: HashMap<NodeID, TcpStream> = HashMap::new();
        type CorrMap = Arc<Mutex<correlation_map>>;

        // Handling incoming messages if different to main.rs
        // Instead of reading from a common/ shared std i/o each node listens on a port for TCP.
        // Via TCP we receive both client messages and peer messages.
        // In handle_connection an incoming TcpStream is sniffed and dispatched to handle either of the
        // `IncomingMessageType`s.
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        for stream in listener.incoming() {
            // Avoid requests being blocked. Each stream gets its own thread.
            // Note: We do not require a handle to wait for the thread, since main never finishes (and therefore cannot drop while thread hasnt finished)
            thread::spawn(move || {
                handle_connection(
                    this_node.clone(),
                    stream.expect("should have a stream"),
                    client_incoming_tx.clone(),
                    raft_incoming_tx.clone(),
                )
            });
        }
    }

    // Supervisor branch: A process, that (re-)spawns Raft nodes (children).
    let env_nodes = env::var("NODES").expect("NODES should be available in ENV.");
    let nodes: Vec<&str> = env_nodes.split(",").collect();

    let exe = env::current_exe()?;
    for node in &nodes {
        let mut peers = nodes.clone();
        peers.retain(|&x| x != *node);
        println!("{:?}", peers);
        let proc = Command::new(&exe)
            .env("IAM", node)
            .env("PEERS", peers.join(","))
            .spawn()?;
    }
    // Supervisor process must not terminate, else child processes are reparented and not killed with Ctrl+C
    loop {}
}
