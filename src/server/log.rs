/// This module abstracts away local log storage
/// ... and eventually snapshotting.

#[derive(Clone)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

enum EntryType {
    Data,
    Read,
    Unknown,
}

pub trait Log: Sync + Send {
    fn get_entry(&self, index: u64) -> &Entry;
    fn get_entries_from(&self, start_index: u64) -> &[Entry];
    fn append_entries(&mut self, entries: Vec<Entry>) -> u64;
    fn append_entry(&mut self, entry: Entry) -> u64;
    fn get_last_entry_index(&self) -> u64;
    fn get_last_entry_term(&self) -> u64;
    ///
    /// Returns true if the log represented by other_log_index,other_log_term is at least as
    /// complete as this log object.
    ///
    /// Completeness is determined by whichever log has a later term. If both logs have the same
    /// last commited term, then whichever log's index is larger is considered more complete
    ///
    fn is_other_log_valid(&self, other_log_index: u64, other_log_term: u64) -> bool;
}

pub struct MemoryLog {
    entries: Vec<Entry>,
}

impl MemoryLog {
    pub fn new() -> MemoryLog {
        MemoryLog { entries: Vec::new() }
    }
}

impl Log for MemoryLog {
    fn get_entry(&self, index:u64) -> &Entry { self.entries.get(index as usize).unwrap()
    }

    fn get_entries_from(&self, start_index: u64) -> &[Entry] {
        &self.entries[start_index as usize .. self.entries.len()]
    }

    fn append_entries(&mut self, entries: Vec<Entry>) -> u64 {
        self.entries.extend(entries); 
        self.get_last_entry_index()
    }

    fn append_entry(&mut self, entry: Entry) -> u64 {
        self.entries.push(entry);
        self.get_last_entry_index()
    }

    fn get_last_entry_index(&self) -> u64 {
        self.entries.len() as u64
    }

    fn get_last_entry_term(&self) -> u64 {
        self.get_entry(self.get_last_entry_index()).term
    }

    // TODO(jason): Add unit tests
    fn is_other_log_valid (&self, other_log_index: u64, other_log_term: u64) -> bool {
        let last_log_term = self.get_last_entry_term();
        other_log_term > last_log_term ||
            (other_log_term == last_log_term &&
            other_log_index >= self.get_last_entry_index())
    }
}

