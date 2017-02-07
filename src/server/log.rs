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
    fn get_entry(&self, index:u64) -> &Entry {
        self.entries.get(index as usize).unwrap()
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
}

