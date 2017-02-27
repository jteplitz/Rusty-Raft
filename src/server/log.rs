use rand::{thread_rng, Rng};
use std::fmt;

///
/// Abstraction for a single Entry for our log.
///
#[derive(Clone)]
pub struct Entry {
    pub index: usize,   // index of this Entry in the log
    pub term: u64,      // term for which this Entry is committed
    pub data: Vec<u8>,  // data blob containing client-defined command to
                        // apply to the client state machine.
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ term: {}, data: {:?} }}", self.term, self.data)
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.term == other.term && self.data == other.data
    }
}

impl Entry {
    ///
    /// Creates a dummy Entry (for testing!) with random data.
    /// TODO (sydli): Could I have Entry implement like a random trait or something?
    ///
    pub fn random () -> Entry {
        let mut vec = vec![0; 8];
        thread_rng().fill_bytes(&mut vec);
        Entry {
            index: 0, // TODO (sydli) this should not be 0
            term: 0,  // Unneeded, for testing, for now.
            data: vec,
        }
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
    /// Will panic if supplied an index exceeds the log length.
    ///
    fn get_entry(&self, index: usize) -> &Entry;

    /// 
    /// Retrieves read-only reference to a slice of all Entries in the log past |start_index|.
    /// Will panic if supplied an index exceeds the log length.
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
    /// Panics if |index| is an invalid log index.
    /// Returns this log object.
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
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog { entries: Vec::new() }
    }
}

impl Log for MemoryLog {
    fn get_entry(&self, index: usize) -> &Entry {
        self.entries.get(index).unwrap()
    }

    fn get_entries_from(&self, start_index: usize) -> &[Entry] {
        &self.entries[start_index .. self.entries.len()]
    }

    fn append_entries(&mut self, entries: Vec<Entry>) -> &Log {
        let mut start_index = self.entries.len();
        let indexed_entries = entries.into_iter().map(|mut entry| {
            entry.index = start_index;
            start_index += 1;
            entry
        }).collect::<Vec<Entry>>();
        self.entries.extend(indexed_entries); 
        self
    }

    fn append_entry(&mut self, mut entry: Entry) -> &Log {
        entry.index = self.entries.len();
        self.entries.push(entry);
        self
    }

    fn get_last_entry_index(&self) -> usize {
        self.entries.len() - 1 as usize
    }

    fn get_last_entry_term(&self) -> u64 {
        self.get_entry(self.get_last_entry_index()).term
    }

    fn roll_back(&mut self, index: usize) -> &Log {
        let destroyed_logs: Vec<_> = self.entries.drain(index + 1..).collect();
        self
    }

    // TODO(jason): Add unit tests
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
    use super::{Entry, Log, MemoryLog};
    fn create_log() -> MemoryLog { MemoryLog::new() } // Replace with other log types as needed
    fn create_filled_log(length: usize) -> MemoryLog {
        let mut log = create_log();
        log.append_entries( vec![Entry::random(); length] );
        log
    }

    #[test]
    #[should_panic]
    /// Makes sure we panic if an incorrect index is supplied to the log.
    fn test_log_get_entry_out_of_index() {
        let mut log = create_log();
        log.append_entry(Entry::random());
        log.get_entry(1); // should panic
    }

    #[test]
    /// Tests that simple append and get works for a two-element array.
    fn test_log_append_get_simple() {
        let mut log = create_log();
        let entry1 = Entry::random();
        let entry2 = Entry::random();

        // Append operations
        log.append_entry(entry1.clone());
        log.append_entry(entry2.clone());

        // Data should be the same
        assert_eq!(entry1.data, log.get_entry(0).data);
        assert_eq!(entry2.data, log.get_entry(1).data);
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn test_log_append_indices_simple() {
        let length = 10;
        let mut log = create_filled_log(length);
        // Make sure the indices are correctly set
        for i in 0..length {
            assert_eq!(log.get_entry(i).index, i);
        }
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn test_log_roll_back_simple() {
        let length = 10;
        let mut log = create_filled_log(length);
        let new_length = 5;
        log.roll_back(new_length);
        // make sure log has been rolled back!
        assert_eq!(log.get_last_entry_index(), new_length);
    }

    #[test]
    #[ignore]
    /// TODO (sydli): Generate a bunch of random queries on the log and make
    ///               sure it stays consistent.
    fn test_fuzz() {
        unimplemented!();
    }
}
