use std::fs::File;
use std::io::{Result, SeekFrom, Error, ErrorKind, Read, BufReader, Seek, BufRead, Cursor, Write};
use std::fs::OpenOptions;

const VOTED_FOR_NONE: &'static str = "NONE";

pub struct StateFile {
    f: File,
    state: Option<State>
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct State {
    term: u64,
    voted_for: Option<u64>
}

impl State {
    /// Serializes the state to a String
    fn serialize (&self) -> String {
        let mut state_str = self.term.to_string() + "\n";

        if self.voted_for.is_none() {
            state_str += VOTED_FOR_NONE;
        } else {
            state_str += &self.voted_for.unwrap().to_string();
        }
        
        state_str += "\n";

        return state_str;
    }
}

impl StateFile {
    /// Opens the file at filename creating it if it doesn't exist
    pub fn new_with_filename(filename: &str) -> Result<StateFile> {
        OpenOptions::new().write(true).read(false).open(filename)
            .map(|f| {
                StateFile {f: f, state: None}
            })
    }

    /// Reads the state in from the file or returns a cached version
    /// May block if the state is not cached in memory
    pub fn get_state(&mut self) -> Result<State> {
        match self.state {
            Some(s) => return Ok(s),
            None => {}
        };

        self.f
            .metadata()
            .and_then(|m| {
                if m.len() == 0 {
                    // there is no persisted state, so return defaults
                    return Ok(State {term: 0, voted_for: None})
                }
                self.f.seek(SeekFrom::Start(0))
                    .and_then(|_| {
                        // clone f so we can create a buffered reader from it and drop the reader
                        // when we're done
                        self.f.try_clone()
                        .and_then(|f| {
                            let mut reader = BufReader::new(f);
                            // the file exists and has bytes
                            // it is considered corrupt if we can't read the term and voted_for from it
                            let term = StateFile::read_term(&mut reader)?;
                            let voted_for = StateFile::read_voted_for(&mut reader)?;

                            Ok(State {term: term, voted_for: voted_for})
                        })
                    })
            })
            .map(|s| {
                self.state = Some(s);
                s
            })
    }

    /// Saves the state to disk. Caching it in memory as well.
    /// Will block until the state has been written. You may assume that if this function
    /// returns an Ok value then the state has been sucesfully written to disk
    pub fn save_state(&mut self, state: State) -> Result<()> {
        let state_str = state.serialize();

        self.f.seek(SeekFrom::Start(0))
        .and_then(|_| {
            self.f.write_all(state_str.as_bytes())
        })
        .and_then(|_| {
            self.f.flush()
        })
        .and_then(|_| {
            self.f.set_len(state_str.len() as u64)
        })
        .and_then(|_| {
            self.f.sync_all()
        })
    }

    fn read_term<R: Read> (reader: &mut BufReader<R>) -> Result<u64> {
        let mut buf = String::new();
        reader.read_line(&mut buf)
        .and_then(|bytes_read| {
            if bytes_read > 0 {
                // (buf - the new line) should now be the term
                buf[0..buf.len() - 1].parse::<u64>()
                .map_err(|_| {
                    Error::new(ErrorKind::InvalidData, "Corrupt term")
                })
            } else {
                Err(Error::new(ErrorKind::InvalidData, "Unable to read term from comfig file"))
            }
        })
    }

    fn read_voted_for<R> (reader: &mut BufReader<R>) -> Result<Option<u64>> 
    where R: Read {
        let mut buf = String::new();
        reader.read_line(&mut buf)
        .and_then(|bytes_read| {
            if bytes_read > 0 {
                if buf[0..buf.len() - 1] == *VOTED_FOR_NONE {
                    Ok(None)
                } else {
                    // (buf - the new line) should now be VotedFor 
                    buf[0..buf.len() - 1].parse::<u64>()
                    .map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Corrupt term")
                    })
                    .map(|id| Some(id))
                }
            } else {
                Err(Error::new(ErrorKind::InvalidData, "Unable to read term from comfig file"))
            }
        })
    }
}

#[test]
fn read_term_reads_term() {
    let term_str = Cursor::new("64\n");
    let mut reader = BufReader::new(term_str);
    let term = StateFile::read_term(&mut reader).unwrap();

    assert_eq!(term, 64);
}

#[test]
fn read_voted_for_reads_voted_for() {
    let voted_for_str  = Cursor::new("8\n");
    let mut reader = BufReader::new(voted_for_str);
    let voted_for = StateFile::read_voted_for(&mut reader).unwrap();

    assert_eq!(voted_for.unwrap(), 8);
}

#[test]
fn read_voted_for_reads_none() {
    let voted_for_str  = Cursor::new(String::from(VOTED_FOR_NONE) + "\n");
    let mut reader = BufReader::new(voted_for_str);
    let voted_for = StateFile::read_voted_for(&mut reader).unwrap();

    assert!(voted_for.is_none());
}

#[test]
fn state_serializes_and_deserializes() {
    let state = State {term: 8, voted_for: Some(12)};
    let state_cursor = Cursor::new(state.serialize());
    let mut reader = BufReader::new(state_cursor);

    let deserialized_term = StateFile::read_term(&mut reader).unwrap();
    let deserialized_voted_for = StateFile::read_voted_for(&mut reader).unwrap();

    assert_eq!(State {term: deserialized_term, voted_for: deserialized_voted_for}, state);
}

#[test]
fn state_serializes_and_deserializes_none_voted_for() {
    let state = State {term: 8, voted_for: None};
    let state_cursor = Cursor::new(state.serialize());
    let mut reader = BufReader::new(state_cursor);

    let deserialized_term = StateFile::read_term(&mut reader).unwrap();
    let deserialized_voted_for = StateFile::read_voted_for(&mut reader).unwrap();

    assert_eq!(State {term: deserialized_term, voted_for: deserialized_voted_for}, state);
}
