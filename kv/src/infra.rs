use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::io::stdin;
use std::sync::{Arc, Mutex};

use json::serde::{Deserialize, Serialize};
use maelstrom::rpc::{Message, Request, ReservedErrorCode, Response};
use maelstrom::{NodeId, NodeMessageIdGenerator};

use crate::{CASError, Store};

pub fn process<K, V, B>(
    Message {
        body,
        // request source is destination for our reply and vice versa
        source: destination,
        destination: source,
    }: Message<B>,
    this: NodeId,
    mut msg_id_gen: NodeMessageIdGenerator,
    store: Arc<Mutex<Store<K, V>>>,
) -> Result<(), Box<dyn Error>>
where
    K: Serialize + Hash + Eq + Debug,
    V: Serialize + PartialEq + Debug,
    B: Into<Request<K, V>>,
{
    // note: inverted!
    eprintln!("processing message from {} for {}", destination, source);

    let req = body.into();
    match req {
        Request::KVRead { key, message_id } => {
            eprintln!("processing kv read (msg id {message_id})");

            let store = store.lock().expect("no poison");
            let msg = Message {
                source: this,
                destination,
                body: match store.read(&key) {
                    Some(v) => Response::KVReadOK {
                        in_reply_to: message_id,
                        value: v,
                        message_id: msg_id_gen
                            .next()
                            .expect("running out of IDs is fatal, reboot"),
                    },
                    None => Response::Error {
                        in_reply_to: message_id,
                        code: ReservedErrorCode::KeyDoesNotExist,
                        text: format!("key: {:?}", key),
                    },
                },
            };

            respond(&msg);

            Ok(())
        }
        Request::KVWrite {
            key,
            value,
            message_id,
        } => {
            eprintln!("processing kv write (msg id {message_id})");

            store.lock().expect("no poison").write(key, value);

            let msg: Message<Response<'_, V>> = Message {
                source: this,
                destination,
                body: Response::KVWriteOK {
                    in_reply_to: message_id,
                    message_id: msg_id_gen
                        .next()
                        .expect("running out of IDs is fatal, reboot"),
                },
            };

            respond(&msg);

            Ok(())
        }
        Request::KVCAS {
            key,
            from,
            to,
            message_id,
        } => {
            eprintln!("processing kv cas (msg id {message_id})");

            let msg: Message<Response<'_, V>> = Message {
                source: this,
                destination,
                body: match store
                    .lock()
                    .expect("no poison")
                    .compare_and_swap(&key, from, to)
                {
                    Ok(()) => Response::KVCASOK {
                        in_reply_to: message_id,
                        message_id: msg_id_gen
                            .next()
                            .expect("running out of IDs is fatal, reboot"),
                    },
                    Err(CASError::NoSuchKey) => Response::Error {
                        in_reply_to: message_id,
                        code: ReservedErrorCode::KeyDoesNotExist,
                        text: format!("no such key: {:?}", key),
                    },
                    Err(CASError::ValueMismatch { requested, found }) => Response::Error {
                        in_reply_to: message_id,
                        code: ReservedErrorCode::PreconditionFailed,
                        text: format!("found existing value: {:?} != {:?}", found, requested),
                    },
                },
            };

            respond(&msg);

            Ok(())
        }
        Request::Init { message_id, .. } => {
            let msg: Message<Response<'_, V>> = Message {
                source: this,
                destination,
                body: Response::Error {
                    in_reply_to: message_id,
                    code: ReservedErrorCode::MalformedRequest,
                    text: "unprocessable request".into(),
                },
            };

            respond(&msg);

            Ok(())
        }
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

/// Responds on stdout.
pub fn respond<B>(resp: &Message<B>)
where
    Message<B>: Serialize,
{
    let resp = resp
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    eprintln!("sending response: {resp}");
    println!("{resp}");
}
