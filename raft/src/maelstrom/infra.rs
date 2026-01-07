use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::stdin;
use std::sync::mpsc::Sender;

use json::serde::{Deserialize, Serialize};

use crate::NodeID;
use crate::maelstrom::rpc::{InitRequest, InitResponse, MessageEnvelope};
use crate::rpc::{ClientMessage, Message, RaftMessage};

pub fn read_and_handle_init(buf: &mut String) -> Result<(NodeID, Vec<NodeID>), Box<dyn Error>> {
    let MessageEnvelope {
        source,
        body:
            InitRequest {
                message_id,
                node_id,
                node_ids,
            },
        ..
    } = read(buf)?;

    send(&MessageEnvelope {
        source: node_id.clone(),
        destination: source,
        body: InitResponse {
            in_reply_to: message_id,
        },
    });

    Ok((node_id, node_ids))
}

/// Reads a line from stdin, parses to JSON, then deserializes into the provided type.
pub fn read<M: Deserialize + Debug>(buf: &mut String) -> Result<M, Box<dyn Error>> {
    stdin().read_line(buf)?;
    eprintln!(
        "got request ({} bytes), raw: {}",
        buf.len(),
        buf.escape_default()
    );

    let jval = json::parse(buf.as_bytes())?;
    eprintln!("parsed json: {}", jval.to_string().escape_default());

    let m = M::deserialize(jval)?;
    eprintln!("deserialized message: {:?}", m);

    buf.clear();

    Ok(m)
}

/// Responds on stdout.
pub fn send<B: Serialize>(msg: &MessageEnvelope<B>) {
    // Don't ask how I found out
    assert_ne!(msg.source, msg.destination, "routing bug: sending to self");

    let msg = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    eprintln!("sending msg: {msg}");
    println!("{msg}");
}

pub fn route_incoming<K, V, B, Cmd>(
    MessageEnvelope {
        source,
        destination,
        body,
    }: MessageEnvelope<B>,
    raft_tx: Sender<(NodeID, RaftMessage<Cmd>)>,
    client_tx: Sender<(NodeID, ClientMessage<K, V>)>,
) where
    K: Serialize + Hash + Eq + Debug + Send + 'static,
    V: Serialize + PartialEq + Debug + Send + 'static,
    Cmd: Serialize + 'static,
    B: Into<Message<K, V, Cmd>>,
{
    eprintln!("processing message from {} for {}", source, destination);

    let msg = body.into();

    match msg {
        Message::Client(
            req @ (ClientMessage::ReadRequest { .. }
            | ClientMessage::WriteRequest { .. }
            | ClientMessage::CASRequest { .. }),
        ) => {
            eprintln!("forwarding client request message");
            client_tx
                .send((source, req))
                .expect("client listener should never hang up");
        }
        Message::Client(
            ClientMessage::ReadResponse { .. }
            | ClientMessage::WriteResponse { .. }
            | ClientMessage::CASResponse { .. }
            | ClientMessage::ErrorResponse { .. },
        ) => {
            eprintln!("error: responses should never be routed to nodes, only ever to clients");
        }
        Message::Raft(msg) => {
            eprintln!("forwarding raft message");
            raft_tx
                .send((source, msg))
                .expect("client listener should never hang up");
        }
    }
}
