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
use std::io::ErrorKind;
use std::io::prelude::*;
use std::net::{Shutdown, TcpStream};
use std::num::NonZeroU16;
use std::str::FromStr;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

// HTTP/1.1 message format according to https://www.rfc-editor.org/info/rfc9112/#section-2
// Messages expected as:
// HTTP-message   = start-line CRLF
//                  *( field-line CRLF )
//                  CRLF
//                  [ message-body ]

// Client-to-node and node-to-node communication happens via tcp.
// Where clients send HTTP/1.1 and nodes serialized RaftMessages (json).
// Unidentified to drop any non-Raft related arrivals.

const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Eq, PartialEq)]
pub enum IncomingMessageType {
    HTTPRequest,
    JSON,
    Unidentified,
}

// Inspired by http crate.
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
    pub fn from_u16(code: u16) -> Self {
        if let Some(num) = NonZeroU16::new(code) {
            StatusCode(num)
        } else {
            unreachable!("should always have valid non-zero u16")
        }
    }
}

// Inspired by http crate.
#[derive(Debug)]
pub struct Response {
    status: StatusCode,
    headers: HashMap<String, String>,
    body: Option<String>,
}

impl<K, V> From<ClientMessage<K, V>> for Response
where
    K: Debug,
    V: ToString + Debug,
{
    fn from(message: ClientMessage<K, V>) -> Self {
        let (code, body) = match message {
            ClientMessage::ReadResponse { value, .. } => (200, Some(value.to_string())),
            ClientMessage::WriteResponse { .. } => (201, None),
            ClientMessage::CASResponse { .. } => (200, None),
            ClientMessage::ErrorResponse { text, .. } => (400, Some(text)),
            msg => (
                500,
                Some(format!(
                    "Internal Server Error (tried to cast other than ClientMessage response: {msg:?})"
                )),
            ),
        };

        let mut headers = HashMap::new();
        if let Some(body) = &body {
            headers.insert("Content-Length".into(), body.len().to_string());
        }
        headers.insert("Connection".into(), "close".into());

        Response {
            status: StatusCode::from_u16(code),
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
            500 => "Internal Server Error",
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
    /// Take a (Tcp)Stream and parse arriving data into the [`Request`] type.
    pub fn from_stream<S: Read + Write>(stream: S) -> Result<Self, std::io::Error> {
        let mut reader = BufReader::new(stream);

        // reading request line
        let mut request = String::new();
        reader.read_line(&mut request)?;
        let request_line: Vec<&str> = request.split_whitespace().collect();
        let incoming_method =
            Method::from_str(request_line.first().expect("should have http method"))?;

        // reading URI
        let incoming_uri: String = (*request_line.get(1).expect("should have uri")).into();

        // reading headers
        let mut incoming_headers: HashMap<String, String> = HashMap::new();
        loop {
            let mut header_element = String::new();
            let incoming_size = reader.read_line(&mut header_element)?;

            if header_element == "\r\n" || incoming_size == 0 {
                break; // end of header
            }

            // compared to split_whitespace which internally calls filter(IsNotEmpty), trimming CLRF before applying split_once
            let Some((key, value)) = header_element.trim().split_once(": ") else {
                continue;
            };

            incoming_headers.insert(key.into(), value.into());
        }

        // reading body if any, requires Content-Length header
        let incoming_body: Option<String> = if incoming_headers.contains_key("Content-Length") {
            let cap: usize = incoming_headers
                .get("Content-Length")
                .ok_or("could not get Content-Length from header")
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?
                .parse()
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidInput, e))?;

            let mut buf_body = String::with_capacity(cap);
            reader
                .by_ref()
                .take(cap as u64)
                .read_to_string(&mut buf_body)?;
            Some(buf_body)
        } else {
            None
        };

        // parsing Request
        Ok(Request {
            method: incoming_method,
            uri: incoming_uri,
            headers: incoming_headers,
            body: incoming_body,
        })
    }

    pub fn get_key(&self) -> Result<&str, std::io::Error> {
        let key = self.uri.strip_prefix("/key/").ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "path must start with /key/")
        })?;

        if key.is_empty() || key.contains('/') {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "key after /key/ in path, not empty or another path",
            ));
        }

        Ok(key)
    }
}

impl TryFrom<Request> for ClientMessage<String, String> {
    type Error = std::io::Error;
    fn try_from(item: Request) -> Result<Self, Self::Error> {
        let id = NodeMessageIDGenerator
            .next()
            .expect("should generate message id")
            .get();
        match item.method {
            Method::Get => Ok(ClientMessage::ReadRequest {
                key: item.get_key()?.into(),
                id,
            }),
            // PUT represents WriteRequests and CASRequests, where CASRequests
            // expect If-Match condition in header.
            Method::Put => match item.headers.contains_key("If-Match") {
                true => Ok(ClientMessage::CASRequest {
                    key: item.get_key()?.into(),
                    from: item
                        .headers
                        .get("If-Match")
                        .ok_or(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "found If-Match but failed to get it",
                        ))?
                        .to_string(),
                    to: item.body.ok_or(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "no body, but required for post",
                    ))?,
                    id,
                }),
                false => Ok(ClientMessage::WriteRequest {
                    key: item.get_key()?.into(),
                    value: item.body.ok_or(std::io::Error::new(
                        ErrorKind::InvalidData,
                        "no body, but required for post",
                    ))?,
                    id,
                }),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum Method {
    Get,
    // A key-value store is idempotent as is PUT: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Methods/PUT
    Put,
}

impl FromStr for Method {
    type Err = std::io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GET" => Ok(Method::Get),
            "PUT" => Ok(Method::Put),
            _ => Err(std::io::Error::new(ErrorKind::InvalidData, s)),
        }
    }
}

/// Peek a TcpStream to sniff the message type.
///
/// Used to differentiate between client-to-node (HTTPRequest), node-to-node (JSON) and arbitrary (Unidentified) traffic.
/// Does not take the stream yet, only checks what type arrived at the socket.
/// Returns an IncomingMessageType or Error if message is not related to Raft.
pub fn sniff_message_type(s: &mut TcpStream) -> Result<IncomingMessageType, std::io::Error> {
    let mut buf = [0u8; 7]; // largest HTTP method
    // Do not wait forever to read data and avoid hanging the thread.
    s.set_read_timeout(Some(IO_TIMEOUT))?;
    s.peek(&mut buf)?;

    if buf.starts_with(b"GET") || buf.starts_with(b"POST") || buf.starts_with(b"PUT") {
        Ok(IncomingMessageType::HTTPRequest)
    } else if buf.starts_with(b"{") {
        Ok(IncomingMessageType::JSON)
    } else {
        Ok(IncomingMessageType::Unidentified)
    }
}

/// Handles traffic arriving at the tcp socket.
///
/// Differentiate between HTTPRequest, JSON and Unindentified to match dispatch into the Raft engine.
/// HTTPRequests become ClientMessages and are send down the thread handling incoming client messages.
/// RaftMessages can be ClientResponses that need to be send down a waiting TcpStream, or RaftMessages
/// for the Raft engine.
/// Unidentified messages are dropped by this handle. We do not care for them on this port.
pub fn handle_connection<K, V, Cmd>(
    id: String,
    mut stream: TcpStream,
    response_queue: &mut Arc<Mutex<HashMap<NodeID, TcpStream>>>,
    client_incoming_tx: Sender<(NodeID, ClientMessage<K, V>)>,
    raft_incoming_tx: Sender<(NodeID, RaftMessage<Cmd>)>,
) -> Result<(), std::io::Error>
where
    K: Serialize + Deserialize + Hash + Eq + Debug + Send + 'static,
    V: Serialize + Deserialize + PartialEq + ToString + Debug + Send + 'static,
    Cmd: Serialize + Deserialize + 'static,
    Request: TryInto<ClientMessage<K, V>, Error = std::io::Error>,
{
    match sniff_message_type(&mut stream)? {
        IncomingMessageType::HTTPRequest => {
            // Sniffed a HTTP request. Request will be parsed into Request type and send
            // over the proper channel to the thread handling incoming client messages.
            let request = Request::from_stream(&stream)?;
            let message: ClientMessage<K, V> = request.try_into()?;
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
            // Sniffed JSON. Try to parse into [`MessageEnvelope`] to dispatch.
            let recv_json: JSONValue = json::Parser::new(&stream)
                .parse()
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            let envelope: MessageEnvelope<Message<K, V, Cmd>> =
                json::serde::Deserialize::deserialize(recv_json)
                    .map_err(|e| std::io::Error::new(ErrorKind::Interrupted, e))?;

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

            // If message is not intended to resurface, send to proper channel for further handling.
            if !is_client_response {
                // Client requests go on the client channel, raft messages on the raft channel.
                route_incoming(envelope, raft_incoming_tx, client_incoming_tx);
                return Ok(());
            }

            // Strip envelope from message. Destination has been reached, source is irrelevant for response.
            let Message::Client(message) = envelope.body else {
                unreachable!("guarded by is_client_response")
            };

            // Build the keys to get the waiting TcpStream from the cache.
            let (msg_id, reply_id) = match &message {
                ClientMessage::ReadResponse {
                    id, in_reply_to, ..
                }
                | ClientMessage::WriteResponse {
                    id, in_reply_to, ..
                }
                | ClientMessage::CASResponse {
                    id, in_reply_to, ..
                }
                | ClientMessage::ErrorResponse {
                    id, in_reply_to, ..
                } => (*id, *in_reply_to),
                msg => unreachable!("only reponses survive as is_client_response, got {msg:?}"),
            };

            let qid_if_follower = format!("node-{}-{}", id, msg_id);
            let qid_if_leader = format!("node-{}-{}", id, reply_id);

            // Pick and remove the waiting TCPStream from the queue, matching either qid.
            let waiting = {
                let mut queue = response_queue
                    .lock()
                    .map_err(|e| std::io::Error::new(ErrorKind::Interrupted, e.to_string()))?;
                queue
                    .remove(&qid_if_follower)
                    .or_else(|| queue.remove(&qid_if_leader))
            };

            match waiting {
                Some(mut client_stream) => {
                    // Build the HTTP response from the client message and send it, then close the connection.
                    let response: Response = message.into();
                    client_stream.write_all(&response.to_wire())?;
                    client_stream.flush()?;
                }
                None => {
                    // Response did not hit the branch to reset message_id via proxy map in incoming_client_rpcs_loop.
                    // Message needs another trip through the engine, in order to swap the message_id
                    // back to the original one held in the proxies map.
                    eprintln!("no stream for {qid_if_follower} or {qid_if_leader}");
                    client_incoming_tx
                        .send((id, message)) // id == envelope.destination
                        .expect("client listener should never hang up");
                }
            }
            Ok(())
        }
        IncomingMessageType::Unidentified => {
            eprintln!("Raft server sniffed an unrelated message on TCP. Message is not processed.");
            Ok(())
        }
    }
}

/// Take a [`MessageEnvelope`] and use minirafts serde crate to serialize into json for tcp transfer.
pub fn send_tcp<B: Serialize>(msg: &MessageEnvelope<B>) -> Result<(), std::io::Error> {
    let payload = msg
        .serialize()
        .expect("all internal types should be serializable")
        .to_string();

    // Node names == tcp ports on localhost.
    let port: u16 = msg.destination.parse().expect("should be parsable");
    eprintln!("sending msg to {}: {payload}", msg.destination);

    // Send MessagEnvelope to destination via tcp
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(payload.as_bytes())?;
    stream.flush()?;
    stream.shutdown(Shutdown::Write)?;

    Ok(())
}

/// A ConnectionLimiter that drops and respawns a limited number of threads.
/// This is not a ThreadPool and hence has some performance overhead, but
/// prevents from spawning unlimited threads on incoming connections.
#[derive(Debug)]
pub struct ConnectionLimiter<T> {
    handles: Vec<JoinHandle<T>>,
    size: usize,
}

impl<T> ConnectionLimiter<T> {
    pub fn new(n: u8) -> Self {
        Self {
            handles: Vec::with_capacity(n as usize),
            size: n as usize,
        }
    }
    pub fn spawn<F>(&mut self, f: F)
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // Try to spawn a new thread, spinning while the ThreadPool is full.
        loop {
            // Clean up Vec of handles, freeing slots of finished threads.
            self.drop_handle_if_free();

            // Check if pool has an open slot and, if so, run the closure on a new thread.
            if self.handles.len() < self.size {
                self.handles.push(thread::spawn(f));
                return;
            }

            thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    // Check if any of the JoinHandles have finished.
    // If yes, drop finished threads' JoinHandles, keeping only running ones.
    fn drop_handle_if_free(&mut self) {
        self.handles.retain(|handle| !handle.is_finished());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::net::TcpListener;

    // Request.new() helper
    fn request(method: Method, uri: &str, headers: &[(&str, &str)], body: Option<&str>) -> Request {
        Request {
            method,
            uri: uri.into(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.map(Into::into),
        }
    }

    // helper to send some data over tcp
    fn server_stream_with(data: &[u8]) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind");
        let addr = listener.local_addr().expect("should have local addr");

        let mut client = TcpStream::connect(addr).expect("should connect");
        client.write_all(data).expect("should write");
        client.flush().expect("should flush");

        let (server, _) = listener.accept().expect("should accept");
        server
    }

    #[test]
    fn method_from_str() {
        assert_eq!(Method::from_str("GET").unwrap(), Method::Get);
        assert_eq!(Method::from_str("PUT").unwrap(), Method::Put);
        assert!(Method::from_str("DELETE").is_err());
    }

    #[test]
    fn from_stream_parses_request_line_headers_and_body() {
        let raw = "PUT /key/foo HTTP/1.1\r\nContent-Length: 3\r\nIf-Match: 1\r\n\r\nbar";
        let req = Request::from_stream(Cursor::new(raw.as_bytes().to_vec())).unwrap();

        assert_eq!(req.method, Method::Put);
        assert_eq!(req.uri, "/key/foo");
        assert_eq!(req.headers.get("Content-Length"), Some(&"3".to_string()));
        assert_eq!(req.headers.get("If-Match"), Some(&"1".to_string()));
        assert_eq!(req.body.as_deref(), Some("bar"));
    }

    #[test]
    fn from_stream_without_content_length_has_no_body() {
        let req =
            Request::from_stream(Cursor::new(b"GET /key/foo HTTP/1.1\r\n\r\n".to_vec())).unwrap();
        assert_eq!(req.method, Method::Get);
        assert_eq!(req.body, None);
    }

    #[test]
    fn get_key_accepts_single_anchored_segment() {
        let req = request(Method::Get, "/key/foo", &[], None);
        assert_eq!(req.get_key().unwrap(), "foo");
    }

    #[test]
    fn get_key_rejects_unanchored_or_missing_prefix() {
        assert!(
            request(Method::Get, "/other/key/foo", &[], None)
                .get_key()
                .is_err()
        );
        assert!(request(Method::Get, "/foo", &[], None).get_key().is_err());
    }

    #[test]
    fn get_key_rejects_empty_or_nested_key() {
        assert!(request(Method::Get, "/key/", &[], None).get_key().is_err());
        assert!(
            request(Method::Get, "/key/a/b", &[], None)
                .get_key()
                .is_err()
        );
    }

    #[test]
    fn sniff_classifies_http_json_and_unidentified() {
        let mut http = server_stream_with(b"GET /key/foo HTTP/1.1\r\n\r\n");
        assert_eq!(
            sniff_message_type(&mut http).unwrap(),
            IncomingMessageType::HTTPRequest
        );

        let mut json = server_stream_with(b"{\"type\":\"append_entries\"}");
        assert_eq!(
            sniff_message_type(&mut json).unwrap(),
            IncomingMessageType::JSON
        );

        let mut other = server_stream_with(b"HTTP/1.1 403 Forbidden");
        assert_eq!(
            sniff_message_type(&mut other).unwrap(),
            IncomingMessageType::Unidentified
        );
    }

    #[test]
    fn get_request_becomes_read_request() {
        let msg: ClientMessage<String, String> = request(Method::Get, "/key/foo", &[], None)
            .try_into()
            .unwrap();
        assert!(matches!(msg, ClientMessage::ReadRequest { key, .. } if key == "foo"));
    }

    #[test]
    fn put_without_if_match_becomes_write_request() {
        let msg: ClientMessage<String, String> = request(Method::Put, "/key/foo", &[], Some("bar"))
            .try_into()
            .unwrap();
        assert!(
            matches!(msg, ClientMessage::WriteRequest { key, value, .. } if key == "foo" && value == "bar")
        );
    }

    #[test]
    fn put_with_if_match_becomes_cas_request() {
        let msg: ClientMessage<String, String> =
            request(Method::Put, "/key/foo", &[("If-Match", "1")], Some("bar"))
                .try_into()
                .unwrap();
        assert!(
            matches!(msg, ClientMessage::CASRequest { key, from, to, .. } if key == "foo" && from == "1" && to == "bar")
        );
    }

    #[test]
    fn put_without_body_is_rejected() {
        let res: Result<ClientMessage<String, String>, _> =
            request(Method::Put, "/key/foo", &[], None).try_into();
        assert!(res.is_err());
    }

    #[test]
    fn read_response_serializes_to_200_with_body() {
        let msg: ClientMessage<String, String> = ClientMessage::ReadResponse {
            in_reply_to: 1,
            value: "bar".into(),
            id: 2,
        };
        let wire = String::from_utf8(Response::from(msg).to_wire()).unwrap();

        assert!(wire.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(wire.contains("Content-Length: 3\r\n"));
        assert!(wire.ends_with("\r\n\r\nbar"));
    }

    #[test]
    fn write_response_serializes_to_201_without_body() {
        let msg: ClientMessage<String, String> = ClientMessage::WriteResponse {
            in_reply_to: 1,
            id: 2,
        };
        let wire = String::from_utf8(Response::from(msg).to_wire()).unwrap();

        assert!(wire.starts_with("HTTP/1.1 201 Created\r\n"));
        assert!(!wire.contains("Content-Length"));
        assert!(wire.ends_with("\r\n\r\n"));
    }

    #[test]
    fn error_response_serializes_to_400_with_text() {
        let msg: ClientMessage<String, String> = ClientMessage::ErrorResponse {
            in_reply_to: 1,
            id: 2,
            code: 22,
            text: "computer says no".into(),
        };
        let wire = String::from_utf8(Response::from(msg).to_wire()).unwrap();

        assert!(wire.starts_with("HTTP/1.1 400 Bad Request\r\n"));
        assert!(wire.ends_with("\r\n\r\ncomputer says no"));
    }
}
