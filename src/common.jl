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

function new_connection(socket::TCPSocket;
                        isclient = false, skip_preface = false)
    settings = HTTPSettings(
                            false, # push_enabled
                            Nullable{UInt}(), # max_concurrent_streams
                            (1 << 15) - 1, # initial_window_size
                            (1 << 24) - 1, # max_frame_size
                            Nullable{UInt}(), # max_header_list_size
                            )

    connection = HTTPConnection(
                                new_dynamic_table(),  # dyanmic_table
                                Vector{HTTPStream}(), # streams
                                (1 << 15) - 1, # window_size
                                isclient,  # isclient
                                isclient ? 1 : 2, # last_stream_identifier
                                settings, # settings
                                false, # closed
                                Channel(32), # channel_act
                                Channel(32), # channel_act_raw
                                Channel(32), # channel_evt
                                Channel(32)  # channel_evt_raw
                                )

    initialize_loop_async(connection, socket, skip_preface = skip_preface)

    connection
end
