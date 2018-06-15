__precompile__()

module GRPC

export ClientChannel, ClientStream

import Base: convert
import Base: open, close, isopen
import Base: put!, take!, fetch, wait
import Base: start, next, done

using HTTP2.Session: HTTPConnection
using HTTP2.Session: new_connection
using HTTP2.Session: ActSendHeaders, ActSendData
using HTTP2.Session: EvtRecvHeaders, EvtRecvData
using HTTP2.Session: put_act!, take_evt!
using HTTP2.Session: new_connection, next_free_stream_identifier

using HttpCommon: Headers

using ProtoBuf: readproto, writeproto
using ProtoBuf: ProtoType

include("common.jl")
include("metadata.jl")

include("client/request.jl")
include("client/channel.jl")
include("client/stream.jl")

include("codegen.jl")

end
