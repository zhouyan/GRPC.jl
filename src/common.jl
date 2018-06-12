import HTTP2.Session: HTTPConnection, ActSendData, EvtGoaway
import ProtoBuf: ProtoType, writeproto, readproto, clear

const CONTENT_TYPE = "application/grpc+proto"
const CONTENT_TYPES = ("application/grpc", "application/grpc+proto")

eval(Expr(:macrocall, Symbol("@enum"), :Status,
          (Expr(:(=), k, v) for (k, v) in
           (
            :OK                  => 0,
            :CANCELLED           => 1,
            :UNKNOWN             => 2,
            :INVALID_ARGUMENT    => 3,
            :DEADLINE_EXCEEDED   => 4,
            :NOT_FOUND           => 5,
            :ALREADY_EXISTS      => 6,
            :PERMISSION_DENIED   => 7,
            :RESOURCE_EXHAUSTED  => 8,
            :FAILED_PRECONDITION => 9,
            :ABORTED             => 10,
            :OUT_OF_RANGE        => 11,
            :UNIMPLEMENTED       => 12,
            :INTERNAL            => 13,
            :UNAVAILABLE         => 14,
            :DATA_LOSS           => 15,
            :UNAUTHENTICATED     => 16,
           )
          )...))

client_streaming(cardinality::Tuple{Bool,Bool}) = cardinality[1]
server_streaming(cardinality::Tuple{Bool,Bool}) = cardinality[2]

struct GRPCError <: Exception
    status::Status
    message::AbstractString

end

function GRPCError(status::Status)
    GRPCError(status, "")
end

struct ProtocolError <: Exception
    message::AbstractString
end

function pack(msg::ProtoType)::Vector{UInt8}
    buf = IOBuffer()
    write(buf, false)
    write(buf, hton(UInt32(0)))
    n = writeproto(buf, msg)
    seek(buf, 1)
    write(buf, hton(UInt32(n)))
    take!(buf)
end

function unpack(msg::Vector{UInt8}, ::Type{T})::T where T <: ProtoType
    buf = IOBuffer(msg)
    if read(buf, Bool)
        throw(ProtocolError("Compression not implemented"))
    end

    len = ntoh(read(buf, UInt32))

    if length(msg) !=  len + 5
        throw(ProtocolError("Incorrect message data size"))
    end

    readproto(buf, T())
end
