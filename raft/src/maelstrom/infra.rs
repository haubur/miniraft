use std::fmt::Debug;
use std::hash::Hash;
use std::io::{self, Write, stdin, stdout};
use std::sync::mpsc::Sender;

use json::serde::{Deserialize, Serialize};

use crate::NodeID;
use crate::maelstrom::rpc::{InitRequest, InitResponse, MessageEnvelope};
use crate::rpc::{ClientMessage, Message, RaftMessage};

pub fn read_and_handle_init(buf: &mut String) -> io::Result<(NodeID, Vec<NodeID>)> {
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
    })?;

    Ok((node_id, node_ids))
}

/// Reads a line from stdin, parses to JSON, then deserializes into the provided type.
pub fn read<M: Deserialize + Debug>(buf: &mut String) -> io::Result<M> {
    stdin().read_line(buf)?;
    eprintln!(
        "got request ({} bytes), raw: {}",
        buf.len(),
        buf.escape_default()
    );

    let jval = json::parse(buf.as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    eprintln!("parsed json: {}", jval.to_string().escape_default());

    let m = M::deserialize(jval).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    eprintln!("deserialized message: {:?}", m);

    buf.clear();

    Ok(m)
}

/// Responds on stdout.
pub fn send<B: Serialize>(msg: &MessageEnvelope<B>) -> io::Result<()> {
    // Don't ask how I found out
    assert_ne!(msg.source, msg.destination, "routing bug: sending to self");

    let msg = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    eprintln!("sending msg: {msg}");
    let mut lock = stdout().lock();
    lock.write_all(msg.as_bytes())?;
    lock.write_all(b"\n")?;
    lock.flush()?;

    Ok(())
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
        Message::Client(msg) => {
            eprintln!("forwarding client message");
            client_tx
                .send((source, msg))
                .expect("client listener should never hang up");
        }
        Message::Raft(msg) => {
            eprintln!("forwarding raft message");
            raft_tx
                .send((source, msg))
                .expect("client listener should never hang up");
        }
    }
}
