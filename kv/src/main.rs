use std::{io::stdin, thread};

use json::serde::{Deserialize, Serialize};
use maelstrom::rpc::{MessageEnvelope, Request, Response};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut s = String::with_capacity(128);
    stdin().read_line(&mut s)?;
    eprintln!("received initial msg: {:?}", s);
    let req = MessageEnvelope::deserialize(json::parse(s.as_bytes())?)?;
    eprintln!("received initial request: {:?}", req);

    #[expect(irrefutable_let_patterns)]
    let MessageEnvelope {
        source,
        body: Request::Init {
            message_id,
            node_id,
            ..
        },
        destination: _,
    } = req
    else {
        return Err("invalid initial request".into());
    };

    println!(
        "{}",
        MessageEnvelope {
            source: node_id,
            destination: source,
            body: Response::InitOk {
                in_reply_to: message_id
            }
        }
        .serialize()?
    );

    loop {
        let mut s = String::with_capacity(128);
        stdin().read_line(&mut s)?;
        eprintln!("got request: {}", s.escape_debug());

        thread::spawn(move || {
            let msg = json::parse(s.as_bytes()).expect("can parse all JSON messages");
            eprintln!("processing: {:?}", msg);
        });
    }
}
