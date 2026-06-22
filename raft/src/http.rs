use std::io::BufRead;
use std::io::BufReader;
use std::net::TcpStream;

pub fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    let mut reader = BufReader::new(&mut stream);
    let mut request = String::new();
    reader.read_line(&mut request)?;
    eprintln!("{}", request);
    Ok(())
}
