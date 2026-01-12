use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::metrics::REGISTERED_METRICS;

/// Serves Prometheus-style metrics on the indicated address, forever.
pub fn serve(addr: impl ToSocketAddrs) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    eprintln!("metrics: will listen on {:?}", listener.local_addr());

    for stream in listener.incoming() {
        // Note, sequential. Should be plenty good enough.
        if let Err(e) = handle_connection(stream?) {
            eprintln!("metrics: failed handling connection: {e}");
        }
    }

    eprintln!("metrics: server shutting down");
    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    eprintln!("metrics: handling connection");

    let mut reader = BufReader::new(&mut stream);
    let mut line = String::with_capacity(32);

    reader.read_line(&mut line)?;

    if !line.starts_with("GET /metrics HTTP/1.1") {
        eprintln!(
            "metrics: unexpected request: {}",
            line.trim().escape_debug()
        );

        stream.write_all(b"HTTP/1.1 404 NOT FOUND\r\n\r\n")?;
        stream.flush()?;
        return Ok(());
    }

    // Drain headers
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 || line.trim().is_empty() {
            break;
        }
    }

    let mut body = String::new();
    {
        let metrics = REGISTERED_METRICS.lock().expect("no poison");
        body.reserve(metrics.len() * 50);

        for metric in metrics.iter() {
            body.push_str(&metric.to_prometheus());
            assert!(body.ends_with('\n'));
        }
    }

    // https://prometheus.io/docs/instrumenting/exposition_formats/#http-content-type-requirements
    write!(
        stream,
        "HTTP/1.1 200 OK\r\n\
        Content-Type: text/plain; version=0.0.4\r\n\
        Content-Length: {}\r\n\
        \r\n",
        body.len()
    )?;

    stream.write_all(body.as_bytes())?;
    stream.flush()
}
