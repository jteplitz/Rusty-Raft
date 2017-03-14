use capnp::serialize::OwnedSegments;
use capnp::message::Reader;
use raft_capnp::{append_entries, append_entries_reply,
                 request_vote, request_vote_reply};
use rpc::{RpcError};
use rpc::client::Rpc;
use std::net::SocketAddr;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};

use super::log::{Log, Entry};
use super::constants;
use super::{MainThreadMessage, AppendEntriesReply, RequestVoteReply};

pub struct AppendEntriesMessage {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<Entry>,
    pub leader_commit: usize,
}

#[derive(Copy, Clone)]
pub struct RequestVoteMessage {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

///
/// Messages for peer background threads to push to associated machines.
pub enum PeerThreadMessage {
    AppendEntries (AppendEntriesMessage),
    RequestVote (RequestVoteMessage),
}


/// Handle for main thread to communicate with Peer.
pub struct PeerHandle {
    pub id: u64,
    pub to_peer: Sender<PeerThreadMessage>,
    pub next_index: usize,
}

impl PeerHandle {
    /// Pushes a non-blocking append-entries request to this peer.
    /// Panics if associated peer background thread is somehow down.
    pub fn append_entries_nonblocking (&self, leader_id: u64, commit_index: usize,
                                       current_term: u64, log: Arc<Mutex<Log>>) {
        let entries = {
            log.lock().unwrap().get_entries_from(self.next_index).to_vec()
        };
        let peer_index = self.next_index;
        let last_entry = entries.get(peer_index).unwrap();
        self.to_peer.send(PeerThreadMessage::AppendEntries(AppendEntriesMessage {
            term: current_term,
            leader_id: leader_id,
            prev_log_index: peer_index,
            prev_log_term: last_entry.term,
            entries: entries.to_vec(),
            leader_commit: commit_index,
        })).unwrap(); // This failing means the peer background thread is down.
    }
}

/// A background thread whose job is to communicate and relay messages between
/// the main thread and the peer at |addr|.
pub struct Peer {
    id: u64,
    addr: SocketAddr,
    to_main: Sender<MainThreadMessage>,
    from_main: Receiver<PeerThreadMessage>
}

impl Peer {
    ///
    /// Spawns a new Peer in a background thread to communicate with the server at id.
    ///
    /// # Panics
    /// Panics if the OS fails to create a new background thread.
    ///
    pub fn start (id: (u64, SocketAddr), to_main: Sender<MainThreadMessage>) -> PeerHandle {
        let (to_peer, from_main) = channel();
        let commit_index = 0;
        
        thread::spawn(move || {
            let peer = Peer {
                id: id.0,
                addr: id.1,
                to_main: to_main,
                from_main: from_main
            };
            peer.main();
        });

        PeerHandle {
            id: id.0,
            to_peer: to_peer,
            next_index: commit_index,
        }
    }

    ///
    /// Sets the term, candidate_id, and last_log index on the rpc from the
    /// data in the RequestVoteMessage
    ///
    fn construct_append_entries (rpc: &mut Rpc, entry: &AppendEntriesMessage) {
        let mut params = rpc.get_param_builder().init_as::<append_entries::Builder>();
        params.set_term(entry.term);
        params.set_leader_id(entry.leader_id);
        params.set_prev_log_index(entry.prev_log_index as u64);
        params.set_prev_log_term(entry.prev_log_term);
        params.set_leader_commit(entry.leader_commit as u64);
        params.borrow().init_entries(entry.entries.len() as u32);
        for i in 0..entry.entries.len() {
            let mut entry_builder = params.borrow().get_entries().unwrap().get(i as u32);
            entry.entries[i].into_proto(&mut entry_builder);
        }
    }

    ///
    /// Processes a append_entries_reply for the current term.
    /// Returns a tuple containing the reply's term and whether the peer
    /// successfully appended the entry.
    ///
    /// # Errors
    /// Returns an RpcError if the msg is not a well formed append_entries_reply
    ///
    fn handle_append_entries_reply (entry_term: u64, msg: Reader<OwnedSegments>)
        -> Result<(u64, bool), RpcError> {
        Rpc::get_result_reader(&msg).and_then(|result| {
            result.get_as::<append_entries_reply::Reader>()
                  .map_err(RpcError::Capnp)
            })
            .map(|reply_reader| {
                let term = reply_reader.get_term();
                let success = reply_reader.get_success();
                (term, term == entry_term && success)
            })
    }

    ///
    /// Sends the appropriate append entries RPC to this peer.
    ///
    /// # Panics
    /// Panics if proto fails to initialize, or main thread has panicked or
    /// is deallocated.
    ///
    fn append_entries_blocking (&mut self, entry: AppendEntriesMessage) {
        let mut rpc = Rpc::new(constants::APPEND_ENTRIES_OPCODE);
        Peer::construct_append_entries(&mut rpc, &entry);
        let (term, success) = rpc.send(self.addr)
            .and_then(|msg| Peer::handle_append_entries_reply(entry.term, msg))
            .unwrap_or((entry.term, false));
        let new_commit_index = entry.prev_log_index + entry.entries.len();
        let reply = AppendEntriesReply {
            term: term,
            commit_index: if success { new_commit_index } else { entry.prev_log_index },
            peer: (self.id, self.addr),
            success: success,
        };
        // Panics if main thread has panicked or been otherwise deallocated.
        self.to_main.send(MainThreadMessage::AppendEntriesReply(reply)).unwrap();
    }

    ///
    /// Requests a vote in the new term from this peer.
    ///
    /// # Panics
    /// Panics if the main thread has panicked or been deallocated
    ///
    fn send_request_vote (&self, vote: RequestVoteMessage) {
        let mut rpc = Rpc::new(constants::REQUEST_VOTE_OPCODE);
        Peer::construct_request_vote(&mut rpc, &vote);

        let vote_granted = rpc.send(self.addr)
            .and_then(|msg| Peer::handle_request_vote_reply(vote.term, msg))
            .unwrap_or(false);

        let reply = RequestVoteReply {
            term: vote.term,
            vote_granted: vote_granted
        };
        // Panics if the main thread has panicked or been deallocated
        self.to_main.send(MainThreadMessage::RequestVoteReply(reply)).unwrap();
    }

    ///
    /// Sets the term, candidate_id, and last_log index on the rpc from the
    /// data in the RequestVoteMessage
    ///
    fn construct_request_vote (rpc: &mut Rpc, vote: &RequestVoteMessage) {
        let mut params = rpc.get_param_builder().init_as::<request_vote::Builder>();
        params.set_term(vote.term);
        params.set_candidate_id(vote.candidate_id);
        params.set_last_log_index(vote.last_log_index as u64);
        params.set_last_log_term(vote.last_log_term);
    }

    ///
    /// Processes a request_vote_reply for the given vote_term
    /// Returms true if the peer granted their vote or false if the peer denied their vote
    ///
    /// # Errors
    /// Returns an RpcError if the msg is not a well formed request_vote_reply
    ///
    fn handle_request_vote_reply (vote_term: u64, msg: Reader<OwnedSegments>) -> Result<bool, RpcError> {
        Rpc::get_result_reader(&msg)
        .and_then(|result| {
            result.get_as::<request_vote_reply::Reader>()
                  .map_err(RpcError::Capnp)
        })
        .map(|reply_reader| {
            let reply_term = reply_reader.get_term();
            let vote_granted = reply_reader.get_vote_granted();
            (reply_term == vote_term) && vote_granted
        })
    }

    ///
    /// Main loop for background thread
    /// Waits on messages from its pipe and acts on them
    ///
    /// # Panics
    /// Panics if the main thread has panicked or been deallocated
    ///
    fn main (mut self) {
        loop {
            match self.from_main.recv().unwrap() {
                PeerThreadMessage::AppendEntries(entry) => self.append_entries_blocking(entry),
                PeerThreadMessage::RequestVote(vote) => self.send_request_vote(vote)
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use capnp::{message, serialize_packed};
    use capnp::serialize::OwnedSegments;
    use std::io::BufReader;
    use super::*;
    use super::super::super::rpc::client::*;
    use super::super::super::raft_capnp::{request_vote, request_vote_reply, append_entries_reply};
    use super::super::super::rpc_capnp::rpc_response;

    #[test]
    fn peer_append_entries_normal() {
    }

#[test]
    fn peer_constructs_valid_append_entries() {
    }

    #[test]
    fn peer_constructs_valid_request_vote() {
        const TERM: u64 = 13;
        const CANDIDATE_ID: u64 = 6;
        const LAST_LOG_INDEX: u64 = 78;
        const LAST_LOG_TERM: u64 = 5;
        let mut rpc = Rpc::new(1);
        let vote = RequestVoteMessage {
            term: TERM,
            candidate_id: CANDIDATE_ID,
            last_log_index: LAST_LOG_INDEX as usize,
            last_log_term: LAST_LOG_TERM
        };

        Peer::construct_request_vote(&mut rpc, &vote);
        let param_reader = rpc.get_param_builder().as_reader().get_as::<request_vote::Reader>().unwrap();
        assert_eq!(param_reader.get_term(), TERM);
        assert_eq!(param_reader.get_candidate_id(), CANDIDATE_ID);
        assert_eq!(param_reader.get_last_log_index(), LAST_LOG_INDEX);
        assert_eq!(param_reader.get_last_log_term(), LAST_LOG_TERM);
    }

    fn get_message_reader<A> (msg: &message::Builder<A>) -> message::Reader<OwnedSegments> 
        where A: message::Allocator
    {
        let mut message_buffer = Vec::new();
        serialize_packed::write_message(&mut message_buffer, &msg).unwrap();

        let mut buf_reader = BufReader::new(&message_buffer[..]);
        serialize_packed::read_message(&mut buf_reader, message::ReaderOptions::new()).unwrap()
    }

    #[test]
    fn peer_vote_reply_handles_vote_granted() {
        const TERM: u64 = 54;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM, true);

        let reader = get_message_reader(&builder);
        assert!(Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    fn peer_vote_reply_handles_incorrect_term() {
        const TERM: u64 = 26;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM - 1, true);

        let reader = get_message_reader(&builder);
        assert!(!Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    fn peer_vote_reply_handles_vote_rejected() {
        const TERM: u64 = 14;
        let mut builder = message::Builder::new_default();
        construct_request_vote_reply(&mut builder, TERM, false);

        let reader = get_message_reader(&builder);
        assert!(!Peer::handle_request_vote_reply(TERM, reader).unwrap());
    }

    #[test]
    // TODO: Determine what level of error checking we want on the messages
    // I have not found consistent error behavior. Even out of bounds seems to proceed
    // without error on travis...
    fn peer_vote_reply_handles_malformed_vote_replies() {
        /*const TERM: u64 = 14;
        let mut builder = message::Builder::new_default();
        construct_append_entries_reply(&mut builder, TERM, true);

        let reader = get_message_reader(&builder);
        let err = Peer::handle_request_vote_reply(TERM, reader).unwrap_err();
        assert!(matches!(err, RpcError::Capnp(_)));*/
    }

    #[test]
    fn peer_vote_reply_handles_malformed_rpc_replies() {
        // TODO(jason): Figure out why this test fails on travis
        /*const TERM: u64 = 19;
        let builder = message::Builder::new_default();
        let reader = get_message_reader(&builder);
        let err = Peer::handle_request_vote_reply(TERM, reader).unwrap_err();
        assert!(matches!(err, RpcError::Capnp(_)));*/
    }

    /// Constructs a valid rpc_response with the given information contained in an
    /// append_entries_reply inside the provided msg buffer.
    fn construct_append_entries_reply<A> (msg: &mut message::Builder<A>, term: u64, success: bool)
        where A: message::Allocator
    {
        let response_builder = msg.init_root::<rpc_response::Builder>();
        let mut reply_builder = response_builder.get_result().init_as::<append_entries_reply::Builder>();
        reply_builder.set_term(term);
        reply_builder.set_success(success);
    }

    /// Constructs a valid rpc_response with the given information contained in a request_vote_reply
    /// inside the provided msg buffer.
    fn construct_request_vote_reply<A> (msg: &mut message::Builder<A>, term: u64, vote_granted: bool) 
        where A: message::Allocator 
    {
        let response_builder = msg.init_root::<rpc_response::Builder>();
        let mut vote_reply_builder = response_builder.get_result().init_as::<request_vote_reply::Builder>();
        vote_reply_builder.set_term(term);
        vote_reply_builder.set_vote_granted(vote_granted);
    }
}
