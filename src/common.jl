import HTTP2.Session: HTTPConnection, ActSendData, EvtGoaway
import HTTP2.Session: take_evt!, put_act!
import ProtoBuf: ProtoType, writeproto, readproto

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

function serialize(msg::ProtoType)
    buf = IOBuffer()
    write(buf, false)
    write(buf, hton(UInt32(0)))
    n = writeproto(buf, msg)
    seek(buf, 1)
    write(buf, hton(UInt32(n)))
    take!(buf)
end

function deserialize(data::Vector{UInt8}, msg::ProtoType)
    buf = IOBuffer(data)
    if read(buf, Bool)
        error("Compression not implemented")
    end
    @assert ntoh(read(buf, UInt32)) == readproto(buf, msg)
end

function send_message(connection::HTTPConnection, stream_id::UInt32,
                      message::ProtoType, is_end = false)
    put_act!(connection, ActSendData(stream_id, serialize(message), is_end))
end

function recv_message(connection::HTTPConnection, ::Type{T}) where T <: ProtoType
    evt = take_evt!(connection)
    if !(evt isa EvtRecvData)
        throw(ProtocolError("Unexpected event $(typeof(evt))"))
    end

    buf = IOBuffer(evt.data)
    if data.size <= 5
        throw(ProtocolError("Unexpected message length $(data.size)"))
    end

    ret = T()
    deserialize(data, ret)
    ret
end
