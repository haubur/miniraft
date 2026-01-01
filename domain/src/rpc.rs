use crate::state::Term;

#[derive(Debug)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: Term,
}
