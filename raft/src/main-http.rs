/// A program that simulates Raft consensus using tcp/http.
///
///
use raft::http::handle_connection;
use std::env;
use std::net::TcpListener;
use std::process::Command;
use std::sync::mpsc;
use std::thread;

fn main() -> Result<(), std::io::Error> {
    println!("main-http here ...");

    // Child branch: A process that runs Raft.
    if let Ok(me) = env::var("IAM") {
        let port: u16 = env::var("IAM").unwrap().parse().unwrap();
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        for stream in listener.incoming() {
            // Avoid requests being blocked. Each stream gets its own thread.
            // Note: We do not require a handle to wait for the thread, since main never finishes (and therefore cannot drop while thread hasnt finished)
            thread::spawn(move || handle_connection(stream.expect("should have a stream")));
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
