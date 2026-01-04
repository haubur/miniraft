use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::stdin;
use std::sync::mpsc::Sender;

use json::serde::{Deserialize, Serialize};

use crate::maelstrom::rpc::{InitRequest, InitResponse, MessageEnvelope, ReservedErrorCode};
use crate::maelstrom::{NodeId, NodeMessageIdGenerator};
use crate::rpc::{KVMessage, Message, RaftMessage};

pub fn read_and_handle_init(buf: &mut String) -> Result<(NodeId, Vec<NodeId>), Box<dyn Error>> {
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

pub fn handle_outgoing<C: Serialize + Clone>(
    this_node: NodeId,
    remote_nodes: &[String],
    msg: crate::rpc::RaftMessage<C>,
) {
    match msg {
        body @ crate::rpc::RaftMessage::RequestVote { .. } => {
            broadcast(this_node.to_string(), remote_nodes, body);
        }
        ref body @ crate::rpc::RaftMessage::RequestVoteResponse { ref remote_id, .. } => {
            send(&MessageEnvelope {
                source: this_node.to_string(),
                destination: remote_id.clone(),
                body,
            })
        }
        _ => unimplemented!(),
    }
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

/// Broadcast a message body from this node to others.
pub fn broadcast<B: Serialize + Clone>(this: NodeId, destinations: &[NodeId], body: B) {
    eprintln!("broadcasting from {} to {:?}", this, destinations);
    for node in destinations {
        send(&MessageEnvelope {
            source: this.clone(),
            destination: node.clone(),
            body: body.clone(),
        });
    }
}

/// Responds on stdout.
pub fn send<B: Serialize>(msg: &MessageEnvelope<B>) {
    let msg = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    eprintln!("sending msg: {msg}");
    println!("{msg}");
}

pub fn handle_incoming<K, V, B, C>(
    MessageEnvelope {
        // request source is destination for our reply and vice versa
        source: destination,
        destination: source,

        body,
    }: MessageEnvelope<B>,
    raft_tx: Sender<RaftMessage<C>>,
    this: NodeId,
    mut msg_id_gen: NodeMessageIdGenerator,
) -> Result<(), Box<dyn Error>>
where
    K: Serialize + Hash + Eq + Debug,
    V: Serialize + PartialEq + Debug,
    C: Serialize + 'static,
    B: Into<Message<K, V, C>>,
{
    // note: inverted!
    eprintln!("processing message from {} for {}", destination, source);

    let msg = body.into();

    match msg {
        Message::KV(KVMessage::ReadRequest { key, message_id }) => {
            eprintln!("processing kv read (msg id {message_id})");

            let msg = MessageEnvelope {
                source: this,
                destination,
                body: Message::<K, V, C>::Error {
                    in_reply_to: Some(message_id),
                    message_id: msg_id_gen.next().expect("should never run out of IDs").0,
                    code: ReservedErrorCode::NotSupported.into(),
                    text: format!("key: {:?}", key),
                },
            };

            send(&msg);

            Ok(())
        }
        Message::KV(KVMessage::WriteRequest {
            key,
            value,
            message_id,
        }) => {
            eprintln!("processing kv write (msg id {message_id})");

            let msg = MessageEnvelope {
                source: this,
                destination,
                body: Message::<K, V, C>::Error {
                    in_reply_to: Some(message_id),
                    message_id: msg_id_gen.next().expect("should never run out of IDs").0,
                    code: ReservedErrorCode::NotSupported.into(),
                    text: format!("key: {:?} / value: {:?}", key, value),
                },
            };

            send(&msg);

            Ok(())
        }
        Message::KV(KVMessage::CASRequest {
            key,
            from,
            to,
            message_id,
        }) => {
            eprintln!("processing kv cas (msg id {message_id})");

            let msg = MessageEnvelope {
                source: this,
                destination,
                body: Message::<K, V, C>::Error {
                    in_reply_to: Some(message_id),
                    message_id: msg_id_gen.next().expect("should never run out of IDs").0,
                    code: ReservedErrorCode::NotSupported.into(),
                    text: format!("key: {:?}, from: {:?}, to: {:?}", key, from, to),
                },
            };

            send(&msg);

            Ok(())
        }
        Message::Raft(msg) => {
            eprintln!("forwarding raft message");
            raft_tx.send(msg)?;
            Ok(())
        }
        _ => unimplemented!(),
    }
}
