@0xdcf9b3ce29f421d0;

struct RaftServer {
  id    @0 :UInt64;
  addr  @1 :Text;
}

# Changes to RaftOp should be reflected in the
# equivalent structures from the common module.
struct RaftCommand {
  struct StateMachineCommand {
    data        @0  :Data;
    session     @1  :SessionInfo;
  }
  union {
    stateMachineCommand  @0 :StateMachineCommand;
    openSession          @1 :UInt64;
    setConfig            @2 :List(RaftServer);
    noop                 @3 :Void;
  }
  struct Reply {
    union {
      stateMachineCommand  @0  :Void;
      openSession          @1  :Void;
      setConfig            @2  :Void;
      noop                 @3  :Void;
    }
  }
}

struct RaftQuery {
  stateMachineQuery   @0   :Data;

  struct Reply {
      stateMachineQuery   @0  :Data;
  }
}

struct ClientRequest {
  union {
    command      @0   :RaftCommand;
    query        @1   :RaftQuery;
    addServer    @2   :RaftServer;
    removeServer @3   :RaftServer;
  }
  struct Reply {
    union {
      error             @0 :RaftError;
      commandReply      @1 :RaftCommand.Reply;
      queryReply        @2 :RaftQuery.Reply;
      addServerReply    @3 :Void;
      removeServerReply @4 :Void;
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

struct SessionInfo {
  clientId       @0  :UInt64;
  sequenceNumber @1  :UInt64;
}

struct RequestVoteReply {
  term          @0   :UInt64;
  voteGranted   @1   :Bool;
}

struct NotLeader {
  union {
    leaderId @0 :UInt64;
    leaderUnknown  @1 :Void;
  }
}

struct RaftError {
  union {
    clientError   @0  :Text;
    notLeader     @1  :NotLeader;
    unknown       @2  :Void;
    sessionError  @3  :Void;
    timeout       @4  :Void;
    ioError       @5  :Text;
  }
}

