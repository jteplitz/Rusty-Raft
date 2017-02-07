@0xdcf9b3ce29f421d0;

struct Entry {
  term          @0   :UInt64;
  index         @1   :UInt64;
  type          @2   :Type;
  data          @3   :Data;
  enum Type {
    data @0;
    read @1;
    unknown @2;
  }
}

struct AppendEntries {
  term          @0   :UInt64;
  leaderId      @1   :UInt64;
  prevLogIndex  @2   :UInt64;
  prevLogTerm   @3   :UInt64;
  entries       @4   :List(Entry);
  leaderCommit  @5   :UInt64;
}

struct AppendEntriesReply {
  term          @0   :UInt64;
  success       @1   :Bool;
}

struct RequestVote {
  term          @0   :UInt64;
  candidateId   @1   :UInt64;
  lastLogIndex  @2   :UInt64;
  lastLogTerm   @3   :UInt64;
}

struct RequestVoteReply {
  term          @0   :UInt64;
  voteGranted   @1   :Bool;
}
