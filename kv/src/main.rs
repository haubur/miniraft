use std::{io::stdin, thread};

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
