// use std::{
//     fs::File,
//     io::{BufReader, BufWriter},
// };

// use domain::state::{Candidate, LogEntry, Persistent, Term};
// use serde::{Deserialize, Serialize};

use std::{io::stdin, thread};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut s = String::with_capacity(128);
        stdin().read_line(&mut s)?;
        eprintln!("got request: {}", s.escape_debug());

        thread::scope(|_scope| {
            eprintln!("processing: {}", s.escape_debug());
        });
    }

    // let mut persistent = Persistent {
    //     current_term: Term(3),
    //     voted_for: Some(Candidate(5)),
    //     log: Vec::<LogEntry<kv::Put>>::new(),
    // };

    // persistent.log = vec![
    //     LogEntry {
    //         cmd: kv::Put {
    //             key: b"foo".into(),
    //             value: b"bar".into(),
    //         },
    //         term: Term(1),
    //     },
    //     LogEntry {
    //         cmd: kv::Put {
    //             key: b"baz".into(),
    //             value: b"rofl".into(),
    //         },
    //         term: Term(7),
    //     },
    // ];

    // println!("created state:\n{:?}", persistent);

    // const PATH: &str = "state.bin";
    // let f = File::create(PATH)?;
    // persistent.serialize(&mut BufWriter::new(f))?;

    // println!("raw state in file:\n{:x?}", std::fs::read(PATH)?.as_slice());

    // let f = File::open(PATH)?;
    // let persistent_deserialized = Persistent::<kv::Put>::deserialize(&mut BufReader::new(f))?;
    // println!("deserialized state:\n{:?}", persistent_deserialized);

    // assert_eq!(persistent, persistent_deserialized);

    // Ok(())
}
