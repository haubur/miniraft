// What do we need to do
// Start Cluster
// Start listen on some port
// Use http-client to read and write data

use raft::http::handle_connection;
use std::net::TcpListener;

fn main() -> Result<(), std::io::Error> {
    println!("main-http here ...");

    let listener = TcpListener::bind("127.0.0.1:7878")?;

    for stream in listener.incoming() {
        handle_connection(stream?).unwrap();
    }
    Ok(())
}
