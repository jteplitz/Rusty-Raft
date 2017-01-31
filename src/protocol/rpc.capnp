@0xa742417d176a08bb;

# TODO: Wrap RpcRequest and RpcResponse in a parent struct, so we can support proper bi-directional rpc
# streams.

struct RpcRequest {
  version @0 :Int16;
  opcode @1 :Int16;
  counter @2 :Int64;

  params @3 :AnyPointer;
}

struct RpcResponse {
  counter @0 :Int64;
  error @1 :Bool;

  result @2 :AnyPointer;
}

struct RpcError {
  # TODO Errors shuold be more than just a message
  msg @0 :Text;
}

# TODO: The following types are only used in tests, and should be put in a seperate file
 
struct MathResult {
  num @0 :Int32;
}

struct MathParams {
  num1 @0 :Int32;
  num2 @1 :Int32;
}

