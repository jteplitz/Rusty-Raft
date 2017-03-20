// Constants
// TODO: Many of these should be overwrittable by Config
pub const ELECTION_TIMEOUT_MIN: u64 = 150; // min election timeout wait value in m.s.
pub const ELECTION_TIMEOUT_MAX: u64 = 300; // min election timeout wait value in m.s.
pub const APPEND_ENTRIES_OPCODE: i16 = 0;
pub const REQUEST_VOTE_OPCODE: i16 = 1;
pub const CLIENT_REQUEST_OPCODE: i16 = 2;
pub const REGISTER_CLIENT_OPCODE: i16 = 3;
