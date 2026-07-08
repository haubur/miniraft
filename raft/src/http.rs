use crate::NodeID;
use crate::maelstrom::NodeMessageIDGenerator;
use crate::maelstrom::rpc::MessageEnvelope;
use crate::rpc::{ClientMessage, RaftMessage};
use json::serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::BufRead;
use std::io::BufReader;
use std::io::prelude::*;
#[cfg(test)]
use std::net::TcpListener;
use std::net::{Shutdown, TcpStream};
use std::num::NonZeroU16;
use std::str::FromStr;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

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

const ALLOWED_STATUS_CODES: [u16; 3] = [200, 201, 400];

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

#[derive(Debug)]
pub struct StatusCode(NonZeroU16);

impl StatusCode {
    pub fn from_u16(code: u16) -> Result<Self, std::io::Error> {
        if ALLOWED_STATUS_CODES.contains(&code) {
            if let Some(num) = NonZeroU16::new(code) {
                return Ok(StatusCode(num));
            }
        }
        panic!("provided code not in allowed list")
    }
}

#[derive(Debug)]
pub struct Response {
    status: StatusCode,
    headers: HashMap<String, String>,
    body: Option<String>,
}

impl<K, V> From<MessageEnvelope<ClientMessage<K, V>>> for Response
where
    // Keys never surface in a response, so `K` is unconstrained beyond the
    // `Debug` needed for the panic below. A response value only needs to render
    // into the HTTP body, hence `ToString`.
    K: Debug,
    V: ToString + Debug,
{
    fn from(envelope: MessageEnvelope<ClientMessage<K, V>>) -> Self {
        let (code, body) = match envelope.body {
            ClientMessage::ReadResponse { value, .. } => (200, Some(value.to_string())),
            ClientMessage::WriteResponse { .. } => (201, None),
            ClientMessage::CASResponse { .. } => (200, None),
            ClientMessage::ErrorResponse { text, .. } => (400, Some(text)),
            msg => unreachable!("requests should never be parsed as a response: got {msg:?}"), // TODO: This is bad design. It's only unreachable if called with correct routing in sniff_message_type.
        };

        let mut headers = HashMap::new();
        if let Some(body) = &body {
            headers.insert("Content-Length".into(), body.len().to_string());
        }
        headers.insert("Connection".into(), "close".into());

        Response {
            status: StatusCode::from_u16(code).expect("mapped codes are in the allow list"),
            headers,
            body,
        }
    }
}

impl Response {
    /// Serialize into the HTTP/1.1 wire format, ready to write onto a TcpStream.
    pub fn to_wire(&self) -> Vec<u8> {
        let reason = match self.status.0.get() {
            200 => "OK",
            201 => "Created",
            400 => "Bad Request",
            _ => "",
        };

        let mut wire = format!("HTTP/1.1 {} {}\r\n", self.status.0.get(), reason);
        for (key, value) in &self.headers {
            wire.push_str(key);
            wire.push_str(": ");
            wire.push_str(value);
            wire.push_str("\r\n");
        }
        wire.push_str("\r\n"); // signals end of header
        if let Some(body) = &self.body {
            wire.push_str(body);
        }

        wire.into_bytes()
    }
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
        let id = NodeMessageIDGenerator
            .next()
            .expect("should generate message id")
            .get();
        match item.method {
            Method::Get => ClientMessage::ReadRequest {
                key: item.get_key().into(),
                id,
            },
            Method::Post => ClientMessage::WriteRequest {
                key: item.get_key().into(),
                value: item.body.expect("should have body"),
                id,
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

/// Peek a TcpStream to sniff the message type.
///
/// Decide if incoming message is HTTP request, serialized ClientMessage (response type) or RaftMessage.
///
/// Assume HTTP request, if message starts with a HTTP verb (here GET, POST, PUT only).
/// Messages that are supposed to be client responses are of type [`ClientMessage`]. They contain either
/// `read_ok`, `write_ok`, `cas_ok` or `error` (compare https://github.com/haubur/miniraft/blob/f25e4091550600b912dcdb49877d48aa8d31f09e/raft/src/rpc.rs#L348).
/// Third, all [`RaftMessage`]s are [`IncomingMessageType::PeerMessage`]s to be forwarded to peer nodes.
pub fn sniff_message_type(s: &mut TcpStream) -> Result<IncomingMessageType, std::io::Error> {
    let mut buf = [0u8; 1024];
    let n = s.peek(&mut buf).expect("stream should be peekable");
    let head = String::from_utf8_lossy(&buf[..n]);
    if head.starts_with("GET") || head.starts_with("POST") || head.starts_with("PUT") {
        return Ok(IncomingMessageType::HTTPRequest);
    } else if head.contains("read_ok")
        || head.contains("write_ok")
        || head.contains("cas_ok")
        || head.contains("error")
    {
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
pub fn handle_connection<K, V, Cmd>(
    id: String,
    mut stream: TcpStream,
    response_queue: &mut Arc<Mutex<HashMap<NodeID, TcpStream>>>,
    client_incoming_tx: Sender<(NodeID, ClientMessage<String, String>)>,
    raft_incoming_tx: Sender<(NodeID, RaftMessage<Cmd>)>,
) -> Result<(), std::io::Error> {
    match sniff_message_type(&mut stream)? {
        IncomingMessageType::HTTPRequest => {
            // Sniffed a HTTP request. Request will be parsed into Request type and send
            // over the proper channel to the thread handling incoming client messages.
            let request = Request::from_stream(&stream);
            let message: ClientMessage<String, String> = request.into();
            eprintln!("{:?}", &message);

            // add request to repsonse_queue to match later occuring response with the correct stream
            let qid = format!("node-{}-{}", id, message.id());
            response_queue
                .lock()
                .expect("should be able to acquire lock")
                .insert(qid, stream);

            // send request to Raft engine thread that handles incoming requests
            client_incoming_tx
                .send((id, message))
                .expect("message should be sendable");
            Ok(())
        }
        IncomingMessageType::HTTPResponse => {
            // Sniffed a serialized ClientMessage. Message will be parsed into Response and sent to the socket
            // which is waiting for this exact response.
            Ok(())
        }
        IncomingMessageType::PeerMessage => {
            unreachable!(); // TODO: forward peer messages over channels
        }
    }
}

pub fn send_tcp<B: Serialize>(msg: &MessageEnvelope<B>) -> std::io::Result<()> {
    // TODO: copied but explain why
    assert_ne!(msg.source, msg.destination, "routing bug: sending to self");

    let payload = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    // Node names == TCP ports on localhost.
    let port: u16 = msg.destination.parse().expect("should be parsable");
    eprintln!("sending msg to {}: {payload}", msg.destination);

    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(payload.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    Ok(())
}

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
