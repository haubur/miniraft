use crate::rpc::ClientMessage;
use std::collections::HashMap;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::prelude::*;
#[cfg(test)]
use std::net::TcpListener;
use std::net::{Shutdown, TcpStream};
use std::str::FromStr;

// HTTP/1.1 message format accroding to https://www.rfc-editor.org/info/rfc9112/#section-2
// Messages expected as:
// HTTP-message   = start-line CRLF
//                  *( field-line CRLF )
//                  CRLF
//                  [ message-body ]
//
// enum HttpMessage {
//     Request(HttpRequest),
//     Response(HttpResponse),
// }

#[derive(Debug, Eq, PartialEq)]
pub enum IncomingMessageType {
    HTTPRequest,
    HTTPResponse,
    PeerMessage,
}

#[derive(Debug)]
pub struct Request {
    method: Method,
    uri: String,
    headers: HashMap<String, String>,
    body: Option<String>,
}

impl Request {
    pub fn from_stream<S: Read + Write>(stream: S) -> Self {
        let mut reader = BufReader::new(stream);

        // reading request line
        let mut request = String::new();
        reader
            .read_line(&mut request)
            .expect("stream should have readable line");
        let request_line: Vec<&str> = request.split_whitespace().collect();
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

        let incoming_body: Option<String> = if incoming_headers.contains_key("Content-Length") {
            let cap: usize = incoming_headers
                .get("Content-Length")
                .expect("should have Content-Length")
                .parse()
                .expect("Content-Length should be parsable");
            let mut buf_body = String::with_capacity(cap);
            loop {
                let mut body_line = String::new();
                let incoming_size = reader
                    .read_line(&mut body_line)
                    .expect("should be valid string");
                eprintln!("{:?}", body_line);

                if incoming_size == 0 {
                    break;
                }
                buf_body.push_str(&body_line);
            }
            Some(buf_body)
        } else {
            None
        };

        // parsing Request
        Request {
            method: incoming_method,
            uri: incoming_uri,
            headers: incoming_headers,
            body: incoming_body,
        }
    }

    pub fn get_key(&self) -> &str {
        let (_, key) = self
            .uri
            .split_once("/key/")
            .expect("should have /key/ in uri");
        key
    }
}

impl From<Request> for ClientMessage<String, String> {
    fn from(item: Request) -> Self {
        match item.method {
            Method::Get => ClientMessage::ReadRequest {
                key: item.get_key().into(),
                id: 0,
            },
            Method::Post => ClientMessage::WriteRequest {
                key: item.get_key().into(),
                value: item.body.expect("should have body"),
                id: 0,
            },
            Method::Put => unreachable!("no put available"),
            Method::Delete => unreachable!("no delete available"),
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

/// Peek a TcpStream to check if its a ClientMessage.
///
/// Peeks the TcpStream and checks for any HTTP verb.
/// If we find any, we have a HTTP ClientMessage on the wire,
/// not a RaftMessage nor an outgoing HTTP ClientMessage.
pub fn sniff_message_type(s: &mut TcpStream) -> Result<IncomingMessageType, std::io::Error> {
    let mut buf = [0u8; 128];
    s.peek(&mut buf).expect("stream should be peekable");
    if buf.starts_with(b"GET") || buf.starts_with(b"POST") || buf.starts_with(b"PUT") {
        return Ok(IncomingMessageType::HTTPRequest);
    } else if buf.starts_with(b"HTTP") {
        return Ok(IncomingMessageType::HTTPResponse);
    } else {
        return Ok(IncomingMessageType::PeerMessage);
    }
}

/// Handles incoming client and peer messages.
///
/// Both client and raft messages land on the TcpStream.
/// Client messages are HTTP. Raft messages have some tbd wire format.
/// For incoming client messages, deserialize into ClientMessage move msg on thread handling incoming client messages.
/// For incoming Raft messages, deserialize into RaftMessage and move msg on thread handling incoming Raft messages.
pub fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    // Need to sniff to check if incoming is ClientMessage or RaftMessage
    match sniff_message_type(&mut stream)? {
        IncomingMessageType::HTTPRequest => {
            let request = Request::from_stream(&stream);
            let message: ClientMessage<String, String> = request.into();
            eprintln!("{:?}", message);
        }
        IncomingMessageType::HTTPResponse => {
            unreachable!(); // TODO: tcp connection correlation
        }
        IncomingMessageType::PeerMessage => {
            unreachable!(); // TODO: forward peer messages over channels
        }
    }

    stream
        // Connection-Header required to enforce Client to close the connection: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Connection_management_in_HTTP_1.x#short-lived_connections
        // HTTPResponse should be a type later with to_bytes or something
        .write_all(b"HTTP/1.1 200\r\n Connection: Close\r\n\r\n")
        .expect("writing data failed");
    stream.shutdown(Shutdown::Both).expect("shut down failed");
    Ok(())
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

    /// Helper to to simulate incoming messages over Tcp.
    fn server_stream_with(data: &[u8]) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind");
        let addr = listener.local_addr().expect("should have local addr");

        let mut client = TcpStream::connect(addr).expect("should connect to addr");
        client.write_all(data).expect("should write test data");
        client.flush().expect("should flush test data");

        let (server, _) = listener.accept().expect("should accept connection");
        server
    }

    #[test]
    fn get_method_from_string() {
        let string_method: &str = "GET";
        assert_eq!(Method::from_str(&string_method).unwrap(), Method::Get);
    }

    #[test]
    fn is_incoming_http_request() {
        let mut stream = server_stream_with(b"GET /key/key-id HTTP/1.1");
        assert_eq!(
            sniff_message_type(&mut stream).unwrap(),
            IncomingMessageType::HTTPRequest
        );
    }

    #[test]
    fn is_not_incoming_http_request() {
        let mut stream = server_stream_with(b"HTTP/1.1 403 Forbidden");
        assert_eq!(
            sniff_message_type(&mut stream).unwrap(),
            IncomingMessageType::HTTPResponse
        );
    }

    #[test]
    fn get_request_from_stream() {
        // Use Cursor to simulate TcpStream
        let test_request: String =
            "GET /key/key-id HTTP/1.1\r\nContent-Length: 26\r\n\r\nsome simple test body data"
                .into();
        let mut buf = Cursor::new(test_request.into_bytes());
        let test_request = Request::from_stream(&mut buf);

        assert_eq!(test_request.method, Method::Get);
        assert_eq!(test_request.uri, "/key/key-id");
        assert_eq!(
            test_request.headers.get("Content-Length"),
            Some(&String::from("26"))
        );
        assert_eq!(test_request.headers.len(), 1);
        assert_eq!(
            test_request.body.clone().unwrap(),
            "some simple test body data"
        );
        assert_eq!(
            test_request.body.unwrap().len(),
            test_request
                .headers
                .get("Content-Length")
                .expect("should have content length")
                .parse::<usize>()
                .expect("Content-Length should be parsable into usize")
        )
    }
}
