use std::sync::atomic::{AtomicU64, Ordering};

use crate::rpc::MessageID;

pub mod infra;
pub mod rpc;

/// Message IDs local to this node. **Unique**. Monotonically increasing. Reset on node
/// reboot. Construct via [`NodeMessageIDGenerator`].
#[derive(Debug)]
pub struct NodeMessageID(MessageID);

impl NodeMessageID {
    /// Read-only.
    pub fn get(&self) -> MessageID {
        self.0
    }
}

/// The current message ID on this node.
static CURRENT_MESSAGE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct NodeMessageIDGenerator;

impl Iterator for NodeMessageIDGenerator {
    type Item = NodeMessageID;

    fn next(&mut self) -> Option<Self::Item> {
        // This is self-consistent so relaxed ordering suffices
        Some(NodeMessageID(
            CURRENT_MESSAGE_ID.fetch_add(1, Ordering::Relaxed),
        ))
    }
}
