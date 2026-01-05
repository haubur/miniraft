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

/// Timeout for elections.
///
/// If we do not receive communications within this period, assume there is no leader.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct Raft<C> {
    /// Underlying raft state.
    state: Arc<Mutex<State<C>>>,
}

impl<C> Raft<C>
where
    C: Deserialize + std::fmt::Debug,
    C: Send + Sync + 'static,
{
    /// Create a new Raft engine from a reader, from which persisted state will be
    /// restored.
    pub fn new_from_src(
        id: NodeID,
        cluster_ids: Vec<NodeID>,
        persistence_source: &mut impl Read,
    ) -> Result<Self, PersistenceError> {
        let state = Persistent::<C>::restore(persistence_source)?;
        Ok(Self::new(id, cluster_ids, state))
    }

    /// Create a new Raft engine.
    ///
    /// Does not do anything by itself; call [`Self::start`] afterwards.
    pub fn new(id: NodeID, cluster_ids: Vec<NodeID>, state: Persistent<C>) -> Self {
        assert!(
            !cluster_ids.contains(&id),
            "cluster IDs should not contain self"
        );

        let state = Arc::new(Mutex::new(State::new(id, cluster_ids, state)));
        Self { state }
    }

    /// Launch the Raft engine.
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
                let outgoing = outgoing.clone();

                move || {
                    // "if many followers become candidates at the same time, votes
                    // could be split so that no candidate obtains a majority. When this
                    // happens, each candidate will time out and start a new election by
                    // incrementing its term and initiating another round"
                    loop {
                        state
                            .lock()
                            .expect("no poison")
                            .maybe_begin_election(outgoing.clone());

                        // Poll as frequently as feasible. Note, the election deadline
                        // this monitors can be bumped forward *at any time*, so we
                        // cannot just sleep once and wake up. So while we don't have
                        // async niceties and to avoid callback hell, just poll.
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            })
            .expect("thread creation should always succeed");

        // Handle incoming messages
        thread::Builder::new()
            .name("raft-handle-incoming-msgs".into())
            .spawn({
                let state = Arc::clone(&self.state);
                let outgoing = outgoing.clone();

                move || {
                    for msg in incoming.iter() {
                        // Check if another node has a more advanced logical clock.
                        let remote_term = msg.term();
                        state
                            .lock()
                            .expect("no poison")
                            .maybe_step_down(remote_term);

                        match msg {
                            rpc::RaftMessage::RequestVote {
                                candidate_id,
                                candidate_term,
                                last_log_index,
                                last_log_term,
                            } => state.lock().expect("no poison").handle_vote_request(
                                outgoing.clone(),
                                candidate_id,
                                candidate_term,
                                last_log_index,
                                last_log_term,
                            ),
                            rpc::RaftMessage::RequestVoteResponse {
                                remote_id,
                                term,
                                vote_granted,
                            } => state.lock().expect("no poison").handle_vote_response(
                                remote_id,
                                term,
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
