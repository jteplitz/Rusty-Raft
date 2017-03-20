@0xdcf9b3ce29f421d0;

enum Op {
  write @0;
  read @1;
  noop @2;
  openSession @3;
  unknown @4;
}

struct Entry {
  term          @0   :UInt64;
  index         @1   :UInt64;
  op            @2   :Op;
  data          @3   :Data;
  session       @4   :SessionInfo;
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

struct ClientRequest {
  op            @0  :Op;
  clientId      @1  :UInt64;
  data          @2  :Data;
  session       @3  :SessionInfo;
}

# To factor into Entry & ClientRequest after merge...
# struct ClientRequest {
#   struct SessionInfo {
#     clientId                 @0  :UInt64;
#     firstOutstandingRequest  @1  :UInt64;
#     requestNumber            @2  :UInt64;
#   }
#   op :union {
#     write :group {
#         command      @0  :Data;
#         session      @1  :SessionInfo;
#     }
#     read :group {
#         query        @2  :Data;
#         session      @3  :SessionInfo;
#     }
#     openSession      @4  :Void;
#   }
# }
# 
# struct ClientReply {
#   struct Error {
#     union {
#       clientError   @0  :Text;
#       notLeader     @1  :Text;
#       sessionError  @2  :Void;
#       rpcError      @3  :Void;
#       unknown       @4  :Void;
#     }
#   }
#   reply :union {
#     error @0 :Error;
#     write @1 :Void;
#     read  @2 :Data;
#   }
# }

struct ClientRequestReply {
  success       @0   :Bool;
  clientId      @1   :UInt64;
  leaderAddr    @2   :Text;
  data          @3   :Data;
}

