use std::sync::atomic::{AtomicU64, Ordering};

pub mod rpc;

/// Node IDs are global and persistent across the lifetime of a node, cf.
/// https://github.com/jepsen-io/maelstrom/blob/cb7f07239012d85d2c0595fd942ddb4613205905/doc/protocol.md#initialization.
pub type NodeId = String;

/// Message IDs local to this node. Monotonically increasing. Reset on node reboot.
/// Construct via [`NodeMessageIdGenerator`].
#[derive(Debug)]
pub struct NodeMessageId(u64);

impl NodeMessageId {
    /// Read-only.
    pub fn get(&self) -> u64 {
        self.0
    }
}

/// The current message ID on this node.
static CURRENT_MESSAGE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct NodeMessageIdGenerator;

impl Iterator for NodeMessageIdGenerator {
    type Item = NodeMessageId;

    fn next(&mut self) -> Option<Self::Item> {
        // This is self-consistent so relaxed ordering suffices
        Some(NodeMessageId(
            CURRENT_MESSAGE_ID.fetch_add(1, Ordering::Relaxed),
        ))
    }
}
