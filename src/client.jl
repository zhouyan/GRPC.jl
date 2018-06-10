import Base: open, close, start, next, done, connect
import HttpCommon: Headers
import HTTP2.Session: HTTPConnection, ActSendHeaders
import HTTP2.Session: EvtRecvHeaders
import HTTP2.Session: put_act!, take_evt!
import HTTP2.Session: new_connection, next_free_stream_identifier
import ProtoBuf: ProtoType

struct ClientChannel
    host::AbstractString
    port::Int
    socket::TCPSocket
    connection::HTTPConnection
    authority::AbstractString

    function ClientChannel(host::AbstractString, port::Integer)
        socket = connect(host, port)
        connection = new_connection(socket, isclient = true)
        new(host, port, socket, connection, "$host:$port")
    end
end

ClientChannel() = ClientChannel("127.0.0.1", 50051)

function ClientChannel(host::AbstractString)
    comps = split(host, ':')
    if length(comps) > 2
        throw(ArgumentError("ClientChannel unable to parse $host"))
    end

    if length(comps) == 1
        port = 50051
    end

    if length(comps) == 2
        host = comps[1]
        port = parse(Int, comps[2])
    end

    ClientChannel(host, port)
end

function send_message(channel::ClientChannel, stream_id, message::ProtoType,
                      is_end = false)
    send_message(channel.connection, stream_id, message, is_end)
end

function recv_message(channel::ClientChannel, ::Type{T}) where T <: ProtoType
    recv_message(channel.connection, T)
end

mutable struct ClientStream
    id::UInt32

    channel::ClientChannel
    request::Request
    send_type::Type
    recv_type::Type

    send_request_done::Bool
    recv_initial_metadata_done::Bool
    recv_trailing_metadata_done::Bool
    cancel_done::Bool
    finished::Bool

    send_message_count::Int
    recv_message_count::Int

    initial_metadata::Nullable{Metadata}
    trailing_metadata::Nullable{Metadata}

    function ClientStream(channel::ClientChannel, request::Request,
                          send_type::Type, recv_type::Type)
        ret = new(0, channel, request, send_type, recv_type,
                  false,  false, false, false, false, 0, 0,
                  Nullable{Metadata}(), Nullable{Metadata}())
        # TODO finalizer
    end
end

# TODO open/close connections and stream

function connect(stream::ClientStream)
    if stream.channel.connection.closed
        stream.channel = ClientChannel(stream.channel.host, stream.channel.port)
    end
end

function send_request(stream::ClientStream)
    if stream.send_request_done
        throw(ProtocalError("Request is already sent"))
    end

    connect(stream)
    stream.id = next_free_stream_identifier(stream.channel.connection)
    put_act!(stream.channel.connection,
             ActSendHeaders(stream.id, stream.request, false))
    stream.send_request_done = true
end

function recv_initial_metadata(stream::ClientStream)
    if !stream.send_request_done
        throw(ProtocolError("Request is not sent yet"))
    end

    if stream.recv_initial_metadata_done
        throw(ProtocolError("Initial metadata was already received"))
    end

    # TODO Deadline

    evt = take_evt!(stream.channel.connection)
    if !(evt isa EvtRecvHeaders)
        throw(ProtocolError("Failed to receive initial metadata. Got $evt"))
    end

    headers = evt.headers
    stream.recv_initial_metadata_done = true
    stream.initial_metadata = Meatadata(headers)

    headrs_map = Dict(headers)
    @assert headers["status"] == "200"
    @assert headers_map["content-type"] in CONTENT_TYPES

    status_code = get(headers, "grpc_status", nothing)
    if !(status_code isa Void)
        status = Status(parse(Int, status_code))
        if status != OK
            status_message = get(headers, "grpc-message", "")
            throw(GRPCError(status, status_message))
        end
    end
end

function recv_trailing_metadata(stream::ClientStream)
    if stream.recv_message_count == 0
        throw(ProtocolError(string("No messages were received before  waiting ",
                                   " for trailing metadata")))
    end

    if stream.recv_trailing_metadata_done
        throw(ProtocolError("Trailing metadta was already received"))
    end

    # TODO Deadline

    evt = take_evt!(stream.channel.connection)
    if !(evt isa EvtRecvHeaders)
        throw(ProtocolError("Failed to receive trailing metadata. Got $evt"))
    end

    headers = evt.headers
    stream.recv_trailing_metadata_done = true
    stream.trailing_metadata = Metadata(headers)

    headers_map = dict(headers)
    status_code = headers_map["grpc-status"]
    status_message = get(headers_map, "grpc-message", "")
    status = Status(parse(Int, status_code))
    if status != OK
        throw(GRPCError(status, status_message))
    end
end

function send_message(stream::ClientStream, message::ProtoType, finish)
    @assert message isa stream.send_type

    if !stream.send_request_done
        send_request(stream)
    end

    if finish && stream.finished
        throw(ProtocolError("Stream already ended"))
    end

    send_message(stream.channel, stream.id, message, finish)

    stream.send_message_count += 1
    if finish
        stream.finished = true
    end
end

function recv_message(stream::ClientStream)
    if !stream.recv_initial_metadata_done
        recv_initial_metadata(stream)
    end

    # TODO Deadline

    message = recv_message(stream.channel, stream.recv_type)
    if message isa Void
        recv_trailing_metadata(stream)
    else
        stream.recv_message_count += 1
    end
    message
end

start(stream::ClientStream) = stream

function next(stream::ClientStream, state)
    recv_message(stream), state
end

function done(stream::ClientStream, state)
    stream.recv_trailing_metadata_done
end

function finish(stream::ClientStream)
    if stream.finished
        throw(ProtocolError("Stream was already ended"))
    end

    # TODO

    stream.finished = true
end

# TODO cancel

function request(channel::ClientChannel, name, request_type, response_type;
                 timeout = Nullable(Number), deadline = Nullable{Deadline}(),
                 metadata = Nullable(Metadata))
    if !isnull(timeout) && isnull(deadline)
        deadline = Nullable(Deadline(get(timeout)))
    elseif !isnull(timeout) && !isnull(deadline)
        deadline = Nullable(min(Deadline(get(timeout)), get(deadline)))
    else
        deadline = Nullable{Deadline}()
    end

    req = Request("POST", "http", name, channel.authority,
                  content_type = CONTENT_TYPE, user_agent = "grpc-julia-GRPC",
                  metadata = metadata, deadline = deadline)

    ClientStream(channel, req, request_type, response_type)
end

struct ServiceMethod
    channel::ClientChannel
    name::AbstractString
    request_type::Type
    response_type::Type
end

function open(func::Function, method::ServiceMethod;
              timeout = Nullable{Number}(),
              metadata = Nullable{Metadata}())
    stream = request(method.channel, method.name,
                     method.request_type, method.response_type,
                     timeout = timeout, metadata = metadata)
    try
        return func(stream)
    finally
        # close(stream) # TODO
    end
end

function unary_unary_method(method, request; kwargs...)
    open(method; kwargs...) do stream
        send_message(stream, request, true)
        recv_message(stream)
    end
end

function unary_stream_method(method, request; kwargs...)
    open(method; kwargs...) do stream
        send_message(stream, request, true)
        for response in stream
            if !(resopnse isa Void)
                put!(channel, response)
            end
        end
    end
end

function stream_unary_methdo(method, request; kwargs...)
    nothing
    open(method; kwargs...) do stream
        for req in request
            send_message(stream, req)
        end
        recv_message(stream)
    end
end
