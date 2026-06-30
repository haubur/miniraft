use std::collections::HashMap;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::prelude::*;
use std::net::{Shutdown, TcpStream};
use std::str::FromStr;

// HTTP/1.1 message format accroding to https://httpwg.org/specs/rfc9112.html
// enum HttpMessage {
//     Request(HttpRequest),
//     Response(HttpResponse),
// }

#[derive(Debug)]
struct Request {
    method: Method,
    uri: String,
    headers: HashMap<String, String>,
    body: String,
}

impl Request {
    pub fn from_stream<S: Read + Write>(stream: &mut S) -> Self {
        let mut reader = BufReader::new(stream);

        // reading request line
        let mut request = String::new();
        reader
            .read_line(&mut request)
            .expect("stream should have readable line");
        eprintln!("{:?}", request);
        let request_line: Vec<&str> = request.split_whitespace().collect();
        eprintln!("{:?}", request_line);
        let incoming_method =
            Method::from_str(request_line[0]).expect("should have valid http method");
        // NOTE: maybe introduce type safe URI
        let incoming_uri: String = request_line[1].into();

        // reading headers
        let mut incoming_headers: HashMap<String, String> = HashMap::new();
        loop {
            let mut header_element = String::new();
            let incoming_size = reader
                .read_line(&mut header_element)
                .expect("should be valid string");

            eprintln!("{:?}", header_element);

            if header_element == "\r\n" || incoming_size == 0 {
                break; // end of header
            }

            // compared to split_whitespace which internally calls filter(IsNotEmpty), trimming CLRF before applying split_once
            let Some((key, value)) = header_element.trim().split_once(": ") else {
                continue;
            };

            incoming_headers.insert(key.into(), value.into());
        }

        eprintln!("{:?}", incoming_headers);

        // reading body
        let mut incoming_body = String::new();
        loop {
            let mut body_line = String::new();
            let incoming_size = reader
                .read_line(&mut body_line)
                .expect("should be valid string");

            if incoming_size == 0 {
                break;
            }

            incoming_body.push_str(&body_line);
        }

        // parsing Request
        Request {
            method: incoming_method,
            uri: incoming_uri,
            headers: incoming_headers,
            body: incoming_body,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl FromStr for Method {
    type Err = std::io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GET" => Ok(Method::Get),
            "POST" => Ok(Method::Post),
            "PUT" => Ok(Method::Put),
            "DELETE" => Ok(Method::Delete),
            _ => panic!("Invalid method {}", s),
        }
    }
}

pub fn handle_connection(mut stream: TcpStream) {
    let mut reader = BufReader::new(&mut stream);
    let mut request = String::new();
    reader.read_line(&mut request).unwrap();
    eprintln!("Received request: {:?}", request);

    // let mut req_iter = request.split_whitespace();

    // match req_iter.next() {
    //     Some("GET") => eprintln!("handling get request"),
    //     Some("PUT") => eprintln!("handling put request"),
    //     _ => eprintln!("nothing"),
    // }

    stream
        // Connection-Header required to enforce Client to close the connection: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Connection_management_in_HTTP_1.x#short-lived_connections
        // HTTPResponse should be a type later with to_bytes or something
        .write_all(b"HTTP/1.1 200\r\n Connection: Close\r\n\r\n")
        .expect("writing data failed");
    stream.shutdown(Shutdown::Both).expect("shut down failed");
}

// Analog to raft/src/maelstrom/infra.rs. Read message from TcpStream instead of stdin
//
// Returns a generic (message) type. Either RaftMessage or ClientMessage.
// pub fn read<M: Deserialize + Debug>(s: TcpStream) -> Result<M, std::io::Error> {
//     // Read stream into a Request type and serialize into RaftMessage/ClientMessage
// }

// Analog to raft/src/maelstrom/infra.rs
// Send message to TcpStream instead of stdout
pub fn send() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_method_from_string() {
        let string_method: &str = "GET";
        assert_eq!(Method::from_str(&string_method).unwrap(), Method::Get);
    }

    #[test]
    fn get_request_from_stream() {
        // use cursor for testing from_stream
        let test_request: String =
            "GET /key/key-id HTTP/1.1\r\nContent-Length: 123\r\nsome simple test body data".into();
        let mut buf = Cursor::new(test_request.into_bytes());
        let test_request = Request::from_stream(&mut buf);

        assert_eq!(test_request.method, Method::Get);
        assert_eq!(test_request.uri, "/key/key-id");
        assert_eq!(
            test_request.headers.get("Content-Length"),
            Some(&String::from("123"))
        );
        assert_eq!(test_request.headers.len(), 1);
    }
}
