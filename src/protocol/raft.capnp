@0xdcf9b3ce29f421d0;

# Changes to RaftOp should be reflected in the
# equivalent structures from the common module.
struct RaftCommand {
  struct StateMachineCommand {
    data        @0  :Data;
    session     @1  :SessionInfo;
  }
  union {
    stateMachineCommand  @0 :StateMachineCommand;
    openSession          @1 :Void;
    setConfig            @2 :Void;
    noop                 @3 :Void;
  }
  struct Reply {
    union {
      stateMachineCommand  @0  :Void;
      openSession          @1  :UInt64;
      setConfig            @2  :Void;
      noop                 @3  :Void;
    }
  }
}

struct RaftQuery {
  union {
    stateMachineQuery   @0   :Data;
    getConfig           @1   :Void;
  }
  struct Reply {
    union {
      stateMachineQuery   @0  :Data;
      getConfig           @1  :Data;
    }
  }
}

struct ClientRequest {
  union {
    command     @0   :RaftCommand;
    query       @1   :RaftQuery;
    unknown     @2   :Void;
  }
  struct Reply {
    union {
      error           @0   :RaftError;
      commandReply    @1   :RaftCommand.Reply;
      queryReply      @2   :RaftQuery.Reply;
    }
  }
}

struct Entry {
  term          @0   :UInt64;
  index         @1   :UInt64;
  op            @2   :RaftCommand;
}

struct AppendEntries {
  term          @0   :UInt64;
  leaderId      @1   :UInt64;
  leaderAddr    @2   :Text;
  prevLogIndex  @3   :UInt64;
  prevLogTerm   @4   :UInt64;
  entries       @5   :List(Entry);
  leaderCommit  @6   :UInt64;
}

struct AppendEntriesReply {
  term          @0   :UInt64;
  success       @1   :Bool;
}

struct RequestVote {
  term          @0   :UInt64;
  candidateId   @1   :UInt64;
  candidateAddr @2   :Text;
  lastLogIndex  @3   :UInt64;
  lastLogTerm   @4   :UInt64;
}

struct SessionInfo {
  clientId       @0  :UInt64;
  sequenceNumber @1  :UInt64;
}

struct RequestVoteReply {
  term          @0   :UInt64;
  voteGranted   @1   :Bool;
}

struct RaftError {
  union {
    clientError   @0  :Text;
    notLeader     @1  :Text;
    sessionError  @2  :Void;
    unknown       @3  :Void;
  }
}

