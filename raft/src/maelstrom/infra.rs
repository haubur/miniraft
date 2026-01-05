use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::stdin;
use std::sync::mpsc::Sender;
use std::thread;

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

pub fn handle_outgoing<C: Serialize + Clone + Send>(
    this_node: NodeId,
    remote_nodes: &[NodeId],
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
pub fn broadcast<B: Serialize + Clone + Send>(this: NodeId, destinations: &[NodeId], body: B) {
    eprintln!("broadcasting from {} to {:?}", this, destinations);

    thread::scope(|scope| {
        for (i, node) in destinations.iter().enumerate() {
            let source = this.clone();
            let destination = node.clone();
            let body = body.clone();

            // "Servers [..] issue RPCs in parallel for best performance." This is a bit
            // pointless sending to STDOUT, but simulate this at least. This should lead
            // to different observed orders from time to time.
            thread::Builder::new()
                .name(format!("broadcast-send-{}-{}", i, node))
                .spawn_scoped(scope, move || {
                    send(&MessageEnvelope {
                        source,
                        destination,
                        body,
                    })
                })
                .expect("thread spawn should always succeed");
        }
    })
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

pub fn handle_incoming<K, V, B, C>(
    MessageEnvelope {
        source,
        destination,
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
    eprintln!("processing message from {} for {}", source, destination);

    let msg = body.into();

    match msg {
        Message::KV(KVMessage::ReadRequest { key, message_id }) => {
            eprintln!("processing kv read (msg id {message_id})");

            let msg = MessageEnvelope {
                source: this,
                destination: source,
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
                destination: source,
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
                destination: source,
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
        Message::Raft(mut msg) => {
            eprintln!("forwarding raft message");

            // If this is a response, need to swap out: the remote ID is *this* node
            // (hence it made it to this point), but for this node's Raft engine it
            // needs to be the remote aka sending node.
            //
            // A very unfortunate wart of the decoupling between Maelstrom and Raft we
            // do.
            msg = match msg {
                RaftMessage::RequestVoteResponse {
                    term, vote_granted, ..
                } => RaftMessage::RequestVoteResponse {
                    remote_id: source,
                    term,
                    vote_granted,
                },
                other => other,
            };

            raft_tx.send(msg)?;
            Ok(())
        }
        _ => unimplemented!(),
    }
}
