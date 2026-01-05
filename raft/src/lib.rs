use std::io::Read;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use json::serde::Deserialize;

use crate::state::{PersistenceError, Persistent, State};

pub mod maelstrom;
pub mod rpc;
pub mod serde;
pub mod state;

/// Identifier for nodes in the cluster.
type NodeID = String;

const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct Raft<C> {
    /// Underlying raft state.
    state: Arc<Mutex<State<C>>>,
    // responses: Sender<rpc::Response>,
}

impl<C> Raft<C>
where
    C: Deserialize + std::fmt::Debug,
    C: Send + Sync + 'static,
{
    pub fn new_from_src(
        id: NodeID,
        persistence_source: &mut impl Read,
    ) -> Result<Self, PersistenceError> {
        let state = Persistent::<C>::restore(persistence_source)?;
        Ok(Self::new(id, state))
    }

    pub fn new(id: NodeID, state: Persistent<C>) -> Self {
        let state = Arc::new(Mutex::new(State::new(id, state)));
        Self { state }
    }

    pub fn start(
        &self,
        incoming: Receiver<rpc::RaftMessage<C>>,
        outgoing: Sender<rpc::RaftMessage<C>>,
    ) {
        // Periodic election launch
        thread::Builder::new()
            .name("raft-election-loop".into())
            .spawn({
                let state = Arc::clone(&self.state);

                move || {
                    loop {
                        state
                            .lock()
                            .expect("no poison")
                            .begin_election(outgoing.clone());

                        thread::sleep(Duration::from_secs(1));
                    }
                }
            })
            .expect("creation should succeed");

        // Handle incoming messages
        thread::Builder::new()
            .name("raft-incoming-msgs".into())
            .spawn({
                let state = Arc::clone(&self.state);

                move || {
                    for msg in incoming.iter() {
                        // Check if another node has a more advanced logical clock.
                        //
                        // TODO: also REJECT if remote term is stale (TBD how RPC should
                        // look like).
                        let remote_term = msg.term();
                        state
                            .lock()
                            .expect("no poison")
                            .maybe_step_down(remote_term);

                        match msg {
                            msg @ rpc::RaftMessage::RequestVote { .. } => {
                                eprintln!("received request for vote: {:?}", msg)
                            }
                            rpc::RaftMessage::RequestVoteResponse {
                                remote_id,
                                remote_term,
                                vote_granted,
                            } => state.lock().expect("no poison").handle_vote_response(
                                remote_id,
                                remote_term,
                                vote_granted,
                            ),
                            _ => unimplemented!("append entries API not implemented yet"),
                        }
                    }

                    panic!("requests sender should never hang up");
                }
            })
            .expect("creation should succeed");
    }
}
