@0xa742417d176a08bb;

struct SortParams {
  nums @0 :List(Int32);
}

struct MathParams {
  num1 @0 :Int32;
  num2 @1 :Int32;
}

struct RpcRequest {
  version @0 :Int16;
  opcode @1 :Int16;
  counter @2 :Int64;

  params :union {
    sort @3 :SortParams;
    math @4 :MathParams;
  }
}

struct SortResult {
  nums @0 :List(Int32);
}

struct MathResult {
  num @0 :Int32;
}

struct RpcResponse {
  counter @0 :Int64;
  error @1 :Bool;

  result :union {
    sort @2 :SortResult;
    math @3 :MathResult;
    errorText @4 :Text;
  }
}
