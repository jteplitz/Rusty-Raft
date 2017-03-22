use std::fmt;
use std::io::{Read, Write, Result, BufReader, BufWriter, Seek, SeekFrom, Error, ErrorKind};
use std::fs::{File, OpenOptions};
use raft_capnp::{entry};
use capnp::serialize_packed;
use capnp::message;
use capnp::message::ReaderOptions;
use super::super::common::{SessionInfo, raft_command};
use super::MainThreadMessage;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Mutex, Arc};
use std::mem;

///
/// Abstraction for a single Entry for our log.
///
#[derive(Clone)]
pub struct Entry {
    pub index: usize,       // index of this Entry in the log
    pub term: u64,          // term for which this Entry is committed
    pub op: raft_command::Request,    // operation represented by this Entry
}

#[cfg(test)]
use rand::{thread_rng, Rng};
#[cfg(test)]
use super::super::common::mock_session;
#[cfg(test)]
pub fn random_entry_with_term(term: u64) -> Entry {
  assert_ne!(term, 0);

  let mut vec = vec![0; 8];
  thread_rng().fill_bytes(&mut vec);
  Entry {
      index: 0,
      term: term,
      op: raft_command::Request::StateMachineCommand {
              data: vec, session: mock_session()},
  }
}

#[cfg(test)]
pub fn random_entries_with_term(size: usize, term: u64) -> Vec<Entry> {
  assert_ne!(term, 0);
  (0 .. size).map(|_| random_entry_with_term(term)).collect::<Vec<Entry>>()
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
            op: raft_command::Request::Noop,
        }
    }

    ///
    /// Deserializes an Entry from its protobuf representation.
    ///
    /// # Panics
    /// Panics if deserialization fails.
    ///
    pub fn from_proto(entry_proto: entry::Reader) -> Entry {
        Entry {
            index: entry_proto.get_index() as usize,
            term: entry_proto.get_term(),
            op: raft_command::request_from_proto(entry_proto.get_op().unwrap()),
        }
    }

    ///
    /// Populates the proto |builder| with information from this entry.
    ///
    pub fn into_proto(&self, builder: &mut entry::Builder) {
        builder.set_term(self.term);
        builder.set_index(self.index as u64);
        raft_command::request_to_proto(self.op.clone(), &mut builder.borrow()
                                .init_op());
    }

    ///
    /// Retrieves a copy of the data within an Entry, if there is any.
    ///
    fn get_data(&self) -> Vec<u8> {
        match self.op {
            raft_command::Request::StateMachineCommand
                {ref data, ..} => data.to_vec(),
            _ => vec![],
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

#[derive(Debug, PartialEq)]
enum BackgroundThreadMessage {
    AppendEntry (Entry),
    Flush,
    Shutdown
}

#[derive(Debug, PartialEq)]
enum BackgroundThreadReply {
    Flushed
}

///
/// Abstraction for a log of "commands" to apply to the client state machine,
/// stored locally.
/// Small wrapper over a list of Entries that handles pushing to persistent storage --
/// eventually should also abstract away automatically snapshotting (TODO)
pub struct Log {
    file: File,
    entries: Vec<Entry>,
    start_index: usize,
    // Stores the byte offset of the first byte each log entry on disk
    entry_offsets: Arc<Mutex<Vec<u64>>>,
    background_thread_tx: Sender<BackgroundThreadMessage>,
    background_thread_rx: Receiver<BackgroundThreadReply>,
    background_thread: Option<JoinHandle<()>>,
    flushed: bool
}

impl Log {
    pub fn new_from_filename(filename: &str, to_main_thread: Sender<MainThreadMessage>) -> Result<Log> {
        let mut f = OpenOptions::new().write(true).read(true).create(true).open(filename)?;
        let (entries, offsets) = Log::read_entries(&mut f)?;
        // start index is always 1 until we implement snapshotting
        let (to_background_thread, from_log_thread) = channel();
        let (to_log_thread, from_background_thread) = channel();
        let f_clone = f.try_clone()?;

        let entry_offsets = Arc::new(Mutex::new(offsets));
        let offsets_clone = entry_offsets.clone();
        let t = thread::spawn(move || Log::background_thread_repl(f_clone, to_main_thread, to_log_thread, from_log_thread, offsets_clone));
        Ok(Log {
            file: f,
            entries: entries,
            start_index: 1,
            entry_offsets: entry_offsets, 
            background_thread_tx: to_background_thread,
            background_thread_rx: from_background_thread,
            background_thread: Some(t),
            flushed: true
        })
    }

    /// 
    /// Retrieves read-only reference to an Entry in the log, specified by |index|.
    /// Returns None if the index is out of bounds
    ///
    pub fn get_entry(&self, index: usize) -> Option<&Entry> {
        if index < self.start_index {
            return None;
        }

        self.entries.get(index - self.start_index)
    }

    /// 
    /// Retrieves read-only reference to a slice of all Entries in the log past |start_index|.
    /// Slice does not include |start_index|
    ///
    pub fn get_entries_from(&self, mut start_index: usize) -> &[Entry] {
        if start_index < self.start_index {
            // We should return a slice containing the full memory log
            start_index = self.start_index - 1;
        }

        start_index -= self.start_index - 1;
        if start_index > self.entries.len() {
            return &[];
        }

        &self.entries[start_index ..]
    }

    ///
    /// Appends a copy of each entry in |entries| to log. Will correctly modify
    /// |index| field in each Entry to match its index in the log.
    /// Returns this log object.
    /// Blocks until these entries are persisted to disk
    ///
    pub fn append_entries_blocking(&mut self, entries: Vec<Entry>) -> Result<&Log> {
        debug_assert!(self.flushed, "Attempt to syncronously push entry into unflushed log");
        let mut start_index = self.entries.len() + self.start_index;
        let indexed_entries = entries.into_iter().map(|mut entry| {
            debug_assert!(entry.term > 0); // can't commit in term 0
            entry.index = start_index;
            start_index += 1;
            entry
        });
        
        // write the entries to disk
        let mut writer = BufWriter::new(&self.file);
        for entry in indexed_entries {
            let mut metadata = self.file.metadata()?;
            let cursor = metadata.len();
            let mut builder = message::Builder::new_default();
            entry.into_proto(&mut builder.init_root::<entry::Builder>());
            serialize_packed::write_message(&mut writer, &builder)?;

            self.entries.push(entry);
            self.entry_offsets.lock().unwrap().push(cursor);
        }

        self.file.sync_all()?;
        Ok(self)
    }


    ///
    /// Appends a copy of |entry| to the log. Will correclty modify 
    /// |index| field in |entry| to match its index in the log.
    /// Returns this log object.
    ///
    /// |entry| will be synced to disk in the background. You must
    /// call flush_background_thread, before attempting to do anymore
    /// IO to the disk on this thread. Specifically you must flush
    /// before calling roll_back or append_entries_blocking
    ///
    ///
    /// #Panics
    /// Panics if the background thread has panicked
    ///
    pub fn append_entry(&mut self, mut entry: Entry) -> &Log {
        debug_assert!(entry.term > 0); // can't commit in term 0
        entry.index = self.entries.len() + self.start_index;
        // TODO(perf): We could wrap entry in an Arc and avoid having to make the copy here
        self.entries.push(entry.clone());

        self.background_thread_tx.send(BackgroundThreadMessage::AppendEntry(entry)).unwrap();

        self.flushed = false;
        self
    }

    ///
    /// Retrieves the index of the last entry in our log.
    ///
    pub fn get_last_entry_index(&self) -> usize {
        self.entries.len() - (self.start_index - 1)
    }

    ///
    /// Retrieves the term of the last entry in our log. 
    ///
    pub fn get_last_entry_term(&self) -> u64 {
        if self.entries.len() == 0 {
            return 0;
        }

        self.get_entry(self.get_last_entry_index())
            .map_or(0, |e| e.term)
    }

    ///
    /// Rolls back log so that the most recent entry is located at |index|.
    /// Returns this log object.
    ///
    /// If this operation rolls back entries that are on disk
    /// this operation will block until the on disk log is truncated
    ///
    /// #Panics
    /// * Panics if you try to roll back entries that are < start_index
    /// because you should never try to roll back snapshotted entries
    /// * Panics if the file on disk has been closed
    ///
    /// #Error
    /// Returns an error if there was an issue rolling back the log on disk
    ///
    pub fn roll_back(&mut self, index: usize) -> Result<&Log> {
        debug_assert!(self.flushed, "Attempt to roll back unflushed log");
        let start_index = index + 1 - self.start_index;
        if start_index >= self.entries.len() {
            return Ok(self); // nothing to remove
        }

        self.entries.drain(start_index ..);

        // we may be rolling back entries that only exist in memory
        let mut entry_offsets = self.entry_offsets.lock().unwrap();
        if start_index < entry_offsets.len() {
            self.file.set_len(entry_offsets[start_index])?;
            entry_offsets.drain(start_index ..);
        }

        Ok(self)
    }

    ///
    /// Returns true if the log represented by other_log_index,other_log_term is at least as
    /// complete as this log object.
    ///
    /// Completeness is determined by whichever log has a later term. If both logs have the same
    /// last commited term, then whichever log's index is larger is considered more complete
    ///
    pub fn is_other_log_valid (&self, other_log_index: usize, other_log_term: u64) -> bool {
        let last_log_term = self.get_last_entry_term();
        other_log_term > last_log_term ||
            (other_log_term == last_log_term &&
            other_log_index >= self.get_last_entry_index())
    }

    /// Blocks until the background thread has been flushed to disk.
    /// This must be called before transitioning away from the leader state
    /// because otherwise the background thread could race with this thread when
    /// writing to the log
    ///
    /// #Panics
    /// Panics if the basckground thread panicked, which can happen if the file
    /// is unwrittable for too long
    pub fn flush_background_thread(&mut self) {
        self.background_thread_tx.send(BackgroundThreadMessage::Flush).unwrap();
        let message = self.background_thread_rx.recv().unwrap();
        match message {
            Flushed => {
                self.flushed = true;
            }
        }
    }

    fn background_thread_repl(mut file: File, to_main_thread: Sender<MainThreadMessage>, to_log_thread: Sender<BackgroundThreadReply>,
                              from_log_thread: Receiver<BackgroundThreadMessage>, entry_offsets: Arc<Mutex<Vec<u64>>>) {
        loop {
            match from_log_thread.recv().unwrap() {
                BackgroundThreadMessage::Flush => {
                    // flush should not be followed by any messages until we reply
                    debug_assert_eq!(from_log_thread.try_recv().unwrap_err(), TryRecvError::Empty);
                    to_log_thread.send(BackgroundThreadReply::Flushed).unwrap();
                },
                BackgroundThreadMessage::AppendEntry(e) => {
                    // Panic if we can't write the entry after trying for MAX_RETRIES
                    let index = e.index;
                    match Log::background_append_entry(&mut file, e, &entry_offsets) {
                        Ok(_) => to_main_thread.send(MainThreadMessage::EntryPersisted(index)).unwrap(),
                        Err(e) => {
                            error!("Unable to write to log file. This is unrecoverable, and the server will shut down. {}", e);
                            panic!("Unable to write to log file.");
                        }
                    };
                },
                BackgroundThreadMessage::Shutdown => break
            }
        }
    }

    /// Tries to write e to file. Puts its start offset into entry_offsets if sucessful.
    ///
    /// #Error
    /// Returns an IO error if it fails to write the entry after 5 retries or if
    /// it fails to flush the entry to disk after writing it
    ///
    /// #Panics
    /// Panics if the offsets vec lock is posioned
    fn background_append_entry(file: &mut File, e: Entry, entry_offsets: &Arc<Mutex<Vec<u64>>>) -> Result<()> {
        const MAX_RETRIES: u32 = 5; // maximum times to try writing an entry before giving up
        let RETRY_WAIT_TIME = Duration::from_millis(50);

        for i in 0..MAX_RETRIES {
            match Log::write_entry_to_disk(file, &e, &entry_offsets) {
                Ok(_) => {
                    break;
                },
                Err(e) => {
                    if i != MAX_RETRIES - 1 {
                        warn!("Unable to write to log file. Will retry.");
                        thread::sleep(RETRY_WAIT_TIME * (i + 1));
                    } else {
                        return Err(e)
                    }
                }
            }
        }

        file.sync_all()
    }

    /// Takes in a file (assumed to be pointed at EOF) and writes the given entry to the file.
    ///
    /// #Panics
    /// Panics if the offsets vec lock is poisioned
    ///
    fn write_entry_to_disk(file: &mut File, e: &Entry, entry_offsets: &Arc<Mutex<Vec<u64>>>) -> Result<()> {
        let mut metadata = file.metadata()?;
        let mut writer = BufWriter::new(file);
        let cursor = metadata.len();

        let mut builder = message::Builder::new_default();
        e.into_proto(&mut builder.init_root::<entry::Builder>());
        serialize_packed::write_message(&mut writer, &builder)?;

        entry_offsets.lock().unwrap().push(cursor);
        Ok(())
    }

    /// Reads all entries from the file into the returned entry vector and
    /// stores their byte offets in the offsets vector.
    /// After this function returns the file's cursor will point to EOF
    ///
    /// #Errors
    /// * Returns an IO error if there are errors reading the from the file.
    /// * Returns a std::io::ErrorKind::InvalidData error if the data in the file is corrupt
    /// * Returns an IO error if there are gaps in the log
    ///
    /// Does not currently check for corrupted entries that are valid
    /// capnproto entries. We could add checksums to do this.
    fn read_entries(f: &mut File) -> Result<(Vec<Entry>, Vec<u64>)> {
        let mut cursor: u64 = 0;
        let mut offsets = vec![];
        let mut entries = vec![];

        let metadata = f.metadata()?;
        let len = metadata.len();
        let mut buf_reader = BufReader::new(f);
        while cursor < len {
            let entry_offset = cursor;
            let entry = serialize_packed::read_message(&mut buf_reader, ReaderOptions::new())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Corrupt captain proto entry while reading"))
            .and_then(|buf| {
                buf_reader.seek(SeekFrom::Current(0))
                .map(|offset| {
                    cursor = offset;
                })?;

                buf.get_root::<entry::Reader>()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "Corrupt captain proto entry while converting"))
                    .map(|e| Entry::from_proto(e))
            })?;
            // TODO Ensure that there are no holes in the log
            // and that it is in order
            entries.push(entry);
            offsets.push(entry_offset);
        }

        Ok((entries, offsets))
    }
}

impl Drop for Log {
    /// Blocks until the background thread shuts down
    ///
    /// #Panics
    /// Panics if the background thread has panicked
    fn drop (&mut self) {
        let thread = mem::replace(&mut self.background_thread, None);
        match thread {
            Some(t) => {
                self.background_thread_tx.send(BackgroundThreadMessage::Shutdown).unwrap();
                t.join().unwrap();
            },
            None => {/* Nothing to shutdown*/}
        }
    }
}

#[cfg(test)]
pub mod mocks {
    use super::*;
    use std::ops::{Deref, DerefMut};
    use std::fs;
    use rand::{thread_rng, Rng};
    use rand::distributions::{IndependentSample, Range};
    use std::sync::mpsc::{Receiver, Sender};
    use super::super::MainThreadMessage;


    /// Wrapper around a test log file that cleans up the file when dropped..
    pub struct MockLogFileHandle{
        pub name: String,
        pub main_thread_rx: Receiver<MainThreadMessage>
    }

    /// Returns a new Log and a handle to the file backing that log on disk.
    /// The file will be automatically deleted when the MockLogFileHandle goes out of scope,
    /// so take care to keep it in scope as long as the Log 
    pub fn new_mock_log() -> (Log, MockLogFileHandle) {
        const LOG_FILENAME_LEN: usize = 20;
        let mut log_filename: String = thread_rng().gen_ascii_chars().take(LOG_FILENAME_LEN).collect();
        log_filename = String::from("/tmp/") + &log_filename;

        let (tx, rx) = channel();
        (Log::new_from_filename(&log_filename, tx).unwrap(), MockLogFileHandle {name: log_filename, main_thread_rx: rx})
    }

    pub fn new_random_with_term(size: usize, term: u64) -> (Log, MockLogFileHandle) {
        let (mut log, _file_handle) = mocks::new_mock_log();
        log.append_entries_blocking(
            (0 .. size - 1).map(|_| random_entry_with_term(term)).collect()).unwrap();
        (log, _file_handle)
    }

    impl Drop for MockLogFileHandle {
        fn drop (&mut self) {
            fs::remove_file(&self.name).unwrap();
        }
    }
}

/// Tests for the log-- these tests should pass for any Log (to test any log, just change the
/// type returned by the |create_log()| helper.
#[cfg(test)]
mod tests {
    use super::super::super::rpc::client::Rpc;
    use super::super::super::raft_capnp::entry;
    use super::{Log, random_entry, random_entry_with_term, random_entries_with_term};
    use super::Entry;
    use super::mocks::{new_mock_log, MockLogFileHandle};
    use super::super::MainThreadMessage;
    use std::sync::mpsc::channel;
    
    fn create_filled_log (length: usize) -> (Log, MockLogFileHandle) {
        let (mut log, _file_handle) = new_mock_log();
        log.append_entries_blocking( vec![random_entry(); length] ).unwrap();
        (log, _file_handle)
    }

    #[test]
    /// Makes sure we return None if the index is out of the log
    fn get_entry_out_of_index() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            log.append_entry(random_entry());
            assert!(log.get_entry(2).is_none());
        }
    }

    #[test]
    fn index_0_is_none() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            assert!(log.get_entry(0).is_none());
            log.append_entry(random_entry());
            assert!(log.get_entry(0).is_none());
        }
    }

    #[test]
    /// Tests that simple append and get works for a two-element array.
    fn append_get_simple() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            let entry1 = random_entry();
            let entry2 = random_entry();

            // Append operations
            log.append_entry(entry1.clone());
            log.append_entry(entry2.clone());

            // Data should be the same
            assert_eq!(entry1.get_data(), log.get_entry(1).unwrap().get_data());
            assert_eq!(entry2.get_data(), log.get_entry(2).unwrap().get_data());
        }
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn append_indices_simple() {
        let length = 10;
        let (log, _file_handle) = create_filled_log(length);
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
        let (log, _file_handle) = create_filled_log(length);
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
        {
            let (log, _file_handle) = new_mock_log();
            assert_eq!(log.get_entries_from(0).len(), 0);
            assert_eq!(log.get_entries_from(1).len(), 0);
        }
        
        {
            let length = 10;
            let (filled_log, _file_handle) = create_filled_log(length);
            assert_eq!(filled_log.get_entries_from(10).len(), 0);
            assert_eq!(filled_log.get_entries_from(11).len(), 0);
        }
    }

    #[test]
    fn get_entries_from_full() {
        let length = 100;
        let (log, _file_handle) = create_filled_log(length);
        assert_eq!(log.get_entries_from(0).len(), length);
    }

    #[test]
    /// Tests that indices stay valid after appending entries.
    fn roll_back_simple() {
        let length = 10;
        let (mut log, _file_handle) = create_filled_log(length);
        let new_length = 5;
        log.roll_back(new_length);
        // make sure log has been rolled back!
        assert_eq!(log.get_last_entry_index(), new_length);
    }

    #[test]
    fn roll_back_works_with_nothing_to_roll_back() {
        let (mut log, _file_handle) = new_mock_log();
        log.roll_back(1);
    }

    #[test]
    fn is_other_log_valid_rejects_previous_terms() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            log.append_entry(random_entry_with_term(1));
            log.append_entry(random_entry_with_term(2));
            assert!(!log.is_other_log_valid(3, 1));
        }
    }

    #[test]
    fn is_other_log_valid_rejects_lower_indices() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            log.append_entry(random_entry_with_term(1));
            log.append_entry(random_entry_with_term(1));
            log.append_entry(random_entry_with_term(2));
            assert!(!log.is_other_log_valid(1, 2));
        }
    }

    #[test]
    fn is_other_log_valid_accepts_same_index() {
        let (mut l, _file_handle) = new_mock_log();
        { // _file_handle must outlive log
            let mut log = l;
            log.append_entry(random_entry_with_term(1));
            assert!(log.is_other_log_valid(1, 1));
        }
    }

    #[test]
    fn is_other_log_valid_accepts_with_empty_log() {
        let (mut log, _file_handle) = new_mock_log();
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

    #[test]
    fn append_entry_writes_to_disk() {
        let (mut log, file_handle) = new_mock_log();
        let entry = random_entry_with_term(1);
        log.append_entry(entry.clone());
        log.flush_background_thread();

        let (tx, rx) = channel();
        let log_from_disk = Log::new_from_filename(&file_handle.name, tx).unwrap();
        assert_eq!(log_from_disk.get_last_entry_index(), 1);
        assert_eq!(*log_from_disk.get_entry(1).unwrap(), entry);
    }

    #[test]
    fn append_entry_notifies_main_thread_when_written() {
        let (mut log, file_handle) = new_mock_log();
        let entry = random_entry_with_term(1);
        log.append_entry(entry.clone());
        let msg = file_handle.main_thread_rx.recv().unwrap();
        assert!(matches!(msg, MainThreadMessage::EntryPersisted(1)));

        let (tx, rx) = channel();
        let log_from_disk = Log::new_from_filename(&file_handle.name, tx).unwrap();
        assert_eq!(log_from_disk.get_last_entry_index(), 1);
        assert_eq!(*log_from_disk.get_entry(1).unwrap(), entry);
    }

    #[test]
    fn append_entries_writes_to_disk() {
        let (mut log, file_handle) = new_mock_log();
        let entries = random_entries_with_term(8, 2);
        log.append_entries_blocking(entries.clone()).unwrap();

        let (tx, rx) = channel();
        let log_from_disk = Log::new_from_filename(&file_handle.name, tx).unwrap();
        assert_eq!(log_from_disk.get_last_entry_index(), 8);
        assert_eq!(log_from_disk.get_entries_from(0).len(), entries.len());
        assert_eq!(log_from_disk.get_entries_from(0), &entries[..]);
    }
}
