use std::fmt;
use raft_capnp::entry;

///
/// Abstraction for a single Entry for our log.
///
#[derive(Clone)]
pub struct Entry {
    pub index: usize,  // index of this Entry in the log
    pub term: u64,     // term for which this Entry is committed
    pub op: Op,        // operation represented by this Entry
}

#[derive(Clone)]
pub enum Op {
    Write (Vec<u8>),
    Read  (Vec<u8>),
    Noop,
    Unknown,
}

#[cfg(test)]
use rand::{thread_rng, Rng};
#[cfg(test)]
pub fn random_entry_with_term(term: u64) -> Entry {
  assert_ne!(term, 0);

  let mut vec = vec![0; 8];
  thread_rng().fill_bytes(&mut vec);
  Entry {
      index: 0,
      term: term,
      op: Op::Write(vec),
  }
}

#[cfg(test)]
pub fn random_entry() -> Entry {
    random_entry_with_term(1)
}

impl Entry {
    ///
    /// Creates an empty entry for the specified term.
    ///
    pub fn noop(term: u64) -> Entry {
        Entry {
            index: 0,
            term: term,
            op: Op::Noop,
        }
    }

    ///
    /// Deserializes an Entry from its protobuf representation.
    ///
    /// # Panics
    /// Panics if deserialization fails.
    ///
    pub fn from_proto(entry_proto: entry::Reader) -> Entry {
        let op = match entry_proto.get_op().unwrap() {
            entry::Op::Write => Op::Write(entry_proto.get_data().unwrap().to_vec()),
            entry::Op::Read => Op::Read(entry_proto.get_data().unwrap().to_vec()),
            entry::Op::Noop => Op::Noop,
            entry::Op::Unknown => Op::Unknown,
        };
        Entry {
            index: entry_proto.get_index() as usize,
            term: entry_proto.get_term(),
            op: op,
        }
    }

    ///
    /// Retrieves the appropriate proto "Op" enum for this entry's |op|.
    ///
    fn get_proto_op(&self) -> entry::Op {
        match self.op {
            Op::Write(_) => entry::Op::Write,
            Op::Read(_) => entry::Op::Read,
            Op::Noop => entry::Op::Noop,
            Op::Unknown => entry::Op::Unknown,
        }
    }

    ///
    /// Populates the proto |builder| with information from this entry.
    ///
    pub fn into_proto(&self, builder: &mut entry::Builder) {
        builder.set_term(self.term);
        builder.set_index(self.index as u64);
        builder.set_data(&self.get_data());
        builder.set_op(self.get_proto_op());
    }

    ///
    /// Retrieves a copy of the data within an Entry, if there is any.
    ///
    fn get_data(&self) -> Vec<u8> {
        match self.op {
            Op::Write(ref data) => data.clone(),
            Op::Read(ref data) => data.clone(),
            Op::Noop => vec![],
            Op::Unknown => vec![],
        }
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ term: {}, data: {:?} }}", self.term, self.get_data())
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.term == other.term && self.get_data() == other.get_data()
    }
}

///
/// Abstraction for a log of "commands" to apply to the client state machine,
/// stored locally.
/// Small wrapper over a list of Entries-- eventually should also abstract away
/// automatically pushing to persistent storage / snapshotting (TODO)
///
pub trait Log: Sync + Send {
    /// 
    /// Retrieves read-only reference to an Entry in the log, specified by |index|.
    /// Returns None if the index is out of bounds
    ///
    fn get_entry(&self, index: usize) -> Option<&Entry>;

    /// 
    /// Retrieves read-only reference to a slice of all Entries in the log past |start_index|.
    /// Slice does not include |start_index|
    ///
    fn get_entries_from(&self, start_index: usize) -> &[Entry];

    ///
    /// Appends a copy of each entry in |entries| to log. Will correctly modify
    /// |index| field in each Entry to match its index in the log.
    /// Returns this log object.
    ///
    fn append_entries(&mut self, entries: Vec<Entry>) -> &Log;

    ///
    /// Appends a copy of |entry| to the log. Will correclty modify 
    /// |index| field in |entry| to match its index in the log.
    /// Returns this log object.
    ///
    fn append_entry(&mut self, entry: Entry) -> &Log;

    ///
    /// Retrieves the index of the last entry in our log.
    ///
    fn get_last_entry_index(&self) -> usize;

    ///
    /// Retrieves the term of the last entry in our log. 
    ///
    fn get_last_entry_term(&self) -> u64;

    ///
    /// Rolls back log so that the most recent entry is located at |index|.
    /// Returns this log object.
    ///
    /// #Panics
    /// Panics if you try to roll back entries that are < start_index
    /// because you should never try to roll back snapshotted entries
    ///
    fn roll_back(&mut self, index: usize) -> &Log;

    ///
    /// Returns true if the log represented by other_log_index,other_log_term is at least as
    /// complete as this log object.
    ///
    /// Completeness is determined by whichever log has a later term. If both logs have the same
    /// last commited term, then whichever log's index is larger is considered more complete
    ///
    fn is_other_log_valid(&self, other_log_index: usize, other_log_term: u64) -> bool;
}

///
/// Simple implementation of Log that stores all logs in memory.
///
pub struct MemoryLog {
    entries: Vec<Entry>,
    start_index: usize
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        // start index is always 1, but will be dynamic after we implement snapshotting
        MemoryLog { entries: Vec::new(), start_index: 1}
    }
}

impl Log for MemoryLog {
    fn get_entry(&self, index: usize) -> Option<&Entry> {
        if index < self.start_index {
            return None;
        }

        self.entries.get(index - self.start_index)
    }

    fn get_entries_from(&self, mut start_index: usize) -> &[Entry] {
        if start_index < self.start_index {
            // We should return a slice containing the full memory log
            // TODO: Will we want snapshotted entries?
            start_index = self.start_index - 1;
        }

        start_index -= self.start_index - 1;
        if start_index > self.entries.len() {
            return &[];
        }

        &self.entries[start_index ..]
    }

    fn append_entries(&mut self, entries: Vec<Entry>) -> &Log {
        let mut start_index = self.entries.len() + self.start_index;
        let indexed_entries = entries.into_iter().map(|mut entry| {
            debug_assert!(entry.term > 0); // can't commit in term 0
            entry.index = start_index;
            start_index += 1;
            entry
        }).collect::<Vec<Entry>>();
        self.entries.extend(indexed_entries); 
        self
    }

    fn append_entry(&mut self, mut entry: Entry) -> &Log {
        debug_assert!(entry.term > 0); // can't commit in term 0
        entry.index = self.entries.len() + self.start_index;
        self.entries.push(entry);
        self
    }

    fn get_last_entry_index(&self) -> usize {
        self.entries.len() - (self.start_index - 1)
    }

    fn get_last_entry_term(&self) -> u64 {
        if self.entries.len() == 0 {
            return 0;
        }

        self.get_entry(self.get_last_entry_index())
            .map_or(0, |e| e.term)
    }

    fn roll_back(&mut self, index: usize) -> &Log {
        let start_index = index - self.start_index + 1;
        if start_index >= self.entries.len() {
            return self; // nothing to remove
        }

        self.entries.drain(start_index ..).collect::<Vec<_>>();
        self
    }

    fn is_other_log_valid (&self, other_log_index: usize, other_log_term: u64) -> bool {
        let last_log_term = self.get_last_entry_term();
        other_log_term > last_log_term ||
            (other_log_term == last_log_term &&
            other_log_index >= self.get_last_entry_index())
    }
}

/// Tests for the log-- these tests should pass for any Log (to test any log, just change the
/// type returned by the |create_log()| helper.
#[cfg(test)]
mod tests {
    use super::super::super::rpc::client::Rpc;
    use super::super::super::raft_capnp::entry;
    use super::{Log, MemoryLog, random_entry, random_entry_with_term};
    use super::Entry;
    fn create_log() -> MemoryLog { MemoryLog::new() } // Replace with other log types as needed

    fn create_filled_log(length: usize) -> MemoryLog {
        let mut log = create_log();
        log.append_entries( vec![random_entry(); length] );
        log
    }

    #[test]
    /// Makes sure we return None if the index is out of the log
    fn get_entry_out_of_index() {
        let mut log = create_log();
        log.append_entry(random_entry());
        assert!(log.get_entry(2).is_none());
    }

    #[test]
    fn index_0_is_none() {
        let mut log = create_log();
        assert!(log.get_entry(0).is_none());
        log.append_entry(random_entry());
        assert!(log.get_entry(0).is_none());
    }

    #[test]
    /// Tests that simple append and get works for a two-element array.
    fn append_get_simple() {
        let mut log = create_log();
        let entry1 = random_entry();
        let entry2 = random_entry();

        // Append operations
        log.append_entry(entry1.clone());
        log.append_entry(entry2.clone());

        // Data should be the same
        assert_eq!(entry1.get_data(), log.get_entry(1).unwrap().get_data());
        assert_eq!(entry2.get_data(), log.get_entry(2).unwrap().get_data());
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn append_indices_simple() {
        let length = 10;
        let log = create_filled_log(length);
        // Make sure the indices are correctly set
        for i in 1..length + 1 {
            assert_eq!(log.get_entry(i).unwrap().index, i);
        }
    }


    #[test]
    /// Tests that get_entries can return a slice 
    /// containing the second half of the log
    fn get_entries_from_simple() {
        let length = 10;
        let log = create_filled_log(length);
        assert_eq!(log.get_last_entry_index(), length);

        let mut curr_index = 6;
        let entries = log.get_entries_from(curr_index - 1);
        assert_eq!(entries.len(), length - (curr_index - 1));

        for entry in entries {
            assert_eq!(entry.index, curr_index);
            curr_index += 1;
        }
    }

    #[test]
    fn get_entries_from_out_of_bounds() {
        let log = create_log();
        assert_eq!(log.get_entries_from(0).len(), 0);
        assert_eq!(log.get_entries_from(1).len(), 0);
        
        let length = 10;
        let filled_log = create_filled_log(length);
        assert_eq!(filled_log.get_entries_from(10).len(), 0);
        assert_eq!(filled_log.get_entries_from(11).len(), 0);
    }

    #[test]
    fn get_entries_from_full() {
        let length = 100;
        let log = create_filled_log(length);
        assert_eq!(log.get_entries_from(0).len(), length);
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn roll_back_simple() {
        let length = 10;
        let mut log = create_filled_log(length);
        let new_length = 5;
        log.roll_back(new_length);
        // make sure log has been rolled back!
        assert_eq!(log.get_last_entry_index(), new_length);
    }

    #[test]
    fn roll_back_works_with_nothing_to_roll_back() {
        let mut log = create_log();
        log.roll_back(1);
    }

    #[test]
    fn is_other_log_valid_rejects_previous_terms() {
        let mut log = create_log();
        log.append_entry(random_entry_with_term(1));
        log.append_entry(random_entry_with_term(2));
        assert!(!log.is_other_log_valid(3, 1));
    }

    #[test]
    fn is_other_log_valid_rejects_lower_indices() {
        let mut log = create_log();
        log.append_entry(random_entry_with_term(1));
        log.append_entry(random_entry_with_term(1));
        log.append_entry(random_entry_with_term(2));
        assert!(!log.is_other_log_valid(1, 2));
    }

    #[test]
    fn is_other_log_valid_accepts_same_index() {
        let mut log = create_log();
        log.append_entry(random_entry_with_term(1));
        assert!(log.is_other_log_valid(1, 1));
    }

    #[test]
    fn is_other_log_valid_accepts_with_empty_log() {
        let log = create_log();
        assert!(log.is_other_log_valid(0, 0));
    }

    #[test]
    fn entry_to_and_from_proto() {
        let entry = random_entry_with_term(5);
        let mut rpc = Rpc::new(1);
        {
            let mut entry_builder = rpc.get_param_builder()
                                       .init_as::<entry::Builder>();
            entry.into_proto(&mut entry_builder);
        }
        let reader = rpc.get_param_builder().as_reader()
                        .get_as::<entry::Reader>().unwrap();
        assert_eq!(entry, Entry::from_proto(reader));
    }
}
