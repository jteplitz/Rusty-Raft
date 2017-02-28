use std::fmt;
use rand::{Rng, thread_rng};

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

#[cfg(test)]
fn random_entry_with_term(term: u64) -> Entry {
  assert_ne!(term, 0);

  let mut vec = vec![0; 8];
  thread_rng().fill_bytes(&mut vec);
  Entry {
      index: 0,
      term: term,
      data: vec,
  }
}

#[cfg(test)]
pub fn random_entry() -> Entry {
    random_entry_with_term(1)
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
    fn get_entry(&self, index: usize) -> Option<&Entry>;

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

    // TODO(jason)
    fn get_entries_from(&self, start_index: usize) -> &[Entry] {
        &self.entries[start_index .. self.entries.len()]
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

    ///
    /// Deletes all entries > index from the log
    ///
    /// #Panics
    /// Panics if you try to roll back entries that are < start_index
    /// because you should never try to roll back snapshotted entries
    ///
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
    use super::{Log, MemoryLog, random_entry, random_entry_with_term};
    fn create_log() -> MemoryLog { MemoryLog::new() } // Replace with other log types as needed

    fn create_filled_log(length: usize) -> MemoryLog {
        let mut log = create_log();
        log.append_entries( vec![random_entry(); length] );
        log
    }

    #[test]
    /// Makes sure we return None if the index is out of the log
    fn test_log_get_entry_out_of_index() {
        let mut log = create_log();
        log.append_entry(random_entry());
        assert!(log.get_entry(2).is_none());
    }

    #[test]
    fn test_log_0_is_none() {
        let mut log = create_log();
        assert!(log.get_entry(0).is_none());
        log.append_entry(random_entry());
        assert!(log.get_entry(0).is_none());
    }

    #[test]
    /// Tests that simple append and get works for a two-element array.
    fn test_log_append_get_simple() {
        let mut log = create_log();
        let entry1 = random_entry();
        let entry2 = random_entry();

        // Append operations
        log.append_entry(entry1.clone());
        log.append_entry(entry2.clone());

        // Data should be the same
        assert_eq!(entry1.data, log.get_entry(1).unwrap().data);
        assert_eq!(entry2.data, log.get_entry(2).unwrap().data);
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn test_log_append_indices_simple() {
        let length = 10;
        let log = create_filled_log(length);
        // Make sure the indices are correctly set
        for i in 1..length + 1 {
            assert_eq!(log.get_entry(i).unwrap().index, i);
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
    fn log_roll_back_works_with_nothing_to_roll_back() {
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
        let mut log = create_log();
        assert!(log.is_other_log_valid(0, 0));
    }

    #[test]
    #[ignore]
    /// TODO (sydli): Generate a bunch of random queries on the log and make
    ///               sure it stays consistent.
    fn test_fuzz() {
        unimplemented!();
    }
}
