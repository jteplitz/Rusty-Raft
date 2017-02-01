@0xdcf9b3ce29f421d0;

struct Entry {
  term          @0   :Int64;
  type          @1   :Type;
  data          @2   :Data;
  enum Type {
    data @0;
    read @1;
    unknown @2;
  }
}

struct AppendEntries {
  term          @0   :Int64;
  leaderId      @1   :Int64;
  prevLogIndex  @2   :Int64;
  prevLogTerm   @3   :Int64;
  entries       @4   :List(Entry);
  leaderCommit  @5   :Int64;
}

struct AppendEntriesReply {
  term          @0   :Int64;
  success       @1   :Bool;
}

struct RequestVote {
  term          @0   :Int64;
  candidateId   @1   :Int64;
  lastLogIndex  @2   :Int64;
  lastLogTerm   @3   :Int64;
}

struct RequestVoteReply {
  term          @0   :Int64;
  voteGranted   @1   :Bool;
}
