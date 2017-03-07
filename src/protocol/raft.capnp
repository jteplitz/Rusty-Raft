@0xdcf9b3ce29f421d0;

struct Entry {
  term          @0   :UInt64;
  index         @1   :UInt64;
  op            @2   :Op;
  data          @3   :Data;
  enum Op {
    write @0;
    read @1;
    noop @2;
    unknown @3;
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

struct ClientRequest {
  op            @0   :Op;
  data          @1   :Data;
  enum Op {
    write @0;
    read @1;
  }
}

struct ClientRequestReply {
  success       @0   :Bool;
  data          @1   :Data;
}

