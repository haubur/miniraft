use crate::NodeID;
use crate::maelstrom::NodeMessageIDGenerator;
use crate::maelstrom::infra::route_incoming;
use crate::maelstrom::rpc::MessageEnvelope;
use crate::rpc::{ClientMessage, Message, RaftMessage};
use json::Value as JSONValue;
use json::serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
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
    JSON,
    Unidentified,
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

impl<K, V> From<ClientMessage<K, V>> for Response
where
    // Keys never surface in a response, so `K` is unconstrained beyond the
    // `Debug` needed for the panic below. A response value only needs to render
    // into the HTTP body, hence `ToString`.
    K: Debug,
    V: ToString + Debug,
{
    fn from(message: ClientMessage<K, V>) -> Self {
        let (code, body) = match message {
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
            reader
                .by_ref()
                .take(cap as u64)
                .read_to_string(&mut buf_body)
                .expect("should read Content-Length bytes of body");
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
/// Since now everything arrives via TCP, peek the stream to derive what kind of message is incoming.
/// - client request (HTTP client)
/// - forwarded client message (MessageEnvelope)
/// - client response (MessageEnvelope)
/// - raft messsage (MesssageEnvelope)
///
/// Returns an IncomingMessageType or Error if message can not be resolved.
pub fn sniff_message_type(s: &mut TcpStream) -> IncomingMessageType {
    let mut buf = [0u8; 7]; // largest HTTP method
    s.peek(&mut buf);

    if buf.starts_with(b"GET") || buf.starts_with(b"POST") || buf.starts_with(b"PUT") {
        IncomingMessageType::HTTPRequest
    } else if buf.starts_with(b"{") {
        IncomingMessageType::JSON
    } else {
        IncomingMessageType::Unidentified
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
    client_incoming_tx: Sender<(NodeID, ClientMessage<K, V>)>,
    raft_incoming_tx: Sender<(NodeID, RaftMessage<Cmd>)>,
) -> Result<(), std::io::Error>
where
    // Union of the bounds each branch requires: `route_incoming` (Serialize +
    // Hash/Eq/PartialEq + Debug + Send + 'static), JSON parsing (Deserialize),
    // the HTTP branch (`Request: Into<ClientMessage>`) and the `Response`
    // conversion (`V: ToString`).
    K: Serialize + Deserialize + Hash + Eq + Debug + Send + 'static,
    V: Serialize + Deserialize + PartialEq + ToString + Debug + Send + 'static,
    Cmd: Serialize + Deserialize + 'static,
    Request: Into<ClientMessage<K, V>>,
{
    match sniff_message_type(&mut stream) {
        IncomingMessageType::HTTPRequest => {
            // Sniffed a HTTP request. Request will be parsed into Request type and send
            // over the proper channel to the thread handling incoming client messages.
            let request = Request::from_stream(&stream);
            let message: ClientMessage<K, V> = request.into();
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
        IncomingMessageType::JSON => {
            // Sniffed JSON. Try to parse into [`MessageEnvelope`] and dispatch.
            let recv_json: JSONValue = json::Parser::new(&stream).parse().expect("should be json");
            let envelope: MessageEnvelope<Message<K, V, Cmd>> =
                json::serde::Deserialize::deserialize(recv_json).expect("should be parseable");

            // Check if received message is a client response that needs to resurface.
            let is_client_response = matches!(
                &envelope.body,
                Message::Client(
                    ClientMessage::ReadResponse { .. }
                        | ClientMessage::WriteResponse { .. }
                        | ClientMessage::CASResponse { .. }
                        | ClientMessage::ErrorResponse { .. }
                )
            );

            // If message is not intended to resurface, send to proper channel to be handled by one of the Raft threads.
            if !is_client_response {
                // Client requests -> client channel, raft messages -> raft channel.
                route_incoming(envelope, raft_incoming_tx, client_incoming_tx);
                return Ok(());
            }

            // Each node keeps client TCP connections open to catch and map reponses to send back.
            // Responses can occur asynchronously on a different node (e.g. the leader).
            // Forward responses to the node which holds the client connection.
            if envelope.destination != id {
                eprintln!("relaying client response to {}", envelope.destination);
                return send_tcp(&envelope);
            }

            // Strip envelope from message. Destination has been reached, source is irrelevant for response.
            let Message::Client(message) = envelope.body else {
                unreachable!("guarded by is_client_response")
            };

            // Build qid to match TCPStream waiting for a response.
            let in_reply_to = match &message {
                ClientMessage::ReadResponse { in_reply_to, .. }
                | ClientMessage::WriteResponse { in_reply_to, .. }
                | ClientMessage::CASResponse { in_reply_to, .. }
                | ClientMessage::ErrorResponse { in_reply_to, .. } => *in_reply_to,
                msg => unreachable!("guarded by is_client_response, got {msg:?}"),
            };
            let qid = format!("node-{}-{}", id, in_reply_to);

            // Build the HTTP Response from the client message and get the bytes to send back.
            let response: Response = message.into();
            let bytes_to_sent = response.to_wire();

            // Pick and remove the waiting TCPStream from the queue.
            // Send the HTTP reponse and close the connection.
            let waiting = response_queue
                .lock()
                .expect("should be able to acquire lock")
                .remove(&qid);
            match waiting {
                Some(mut client_stream) => {
                    client_stream.write_all(&bytes_to_sent)?;
                    client_stream.flush()?;
                }
                None => eprintln!("no client awaiting response {qid}, dropping"),
            }
            Ok(())
        }
        IncomingMessageType::Unidentified => {
            // Sniffed an unrelated message.
            Ok(())
        }
    }
}

pub fn send_tcp<B: Serialize>(msg: &MessageEnvelope<B>) -> std::io::Result<()> {
    let payload = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    // Node names == TCP ports on localhost.
    let port: u16 = msg.destination.parse().expect("should be parsable");
    eprintln!("sending msg to {}: {payload}", msg.destination);

    // Send MessagEnvelope to destination via TCP
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(payload.as_bytes())?;
    stream.flush()?;
    stream.shutdown(Shutdown::Write)?;

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
