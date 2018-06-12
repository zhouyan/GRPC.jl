import Base: put!, take!, fetch, isopen, collect, start, next, done
import HttpCommon: Headers
import HTTP2.Session: HTTPConnection
import HTTP2.Session: ActSendHeaders, ActSendData
import HTTP2.Session: EvtRecvHeaders, EvtRecvData, EvtGoaway
import HTTP2.Session: new_connection, next_free_stream_identifier
import HTTP2.Session: put_act!, take_evt!
import ProtoBuf: ProtoType

################################################################################
# Channel, manage raw connection
################################################################################

struct ClientChannel
    host::AbstractString
    port::Int
    socket::TCPSocket
    connection::HTTPConnection
    authority::AbstractString
    receiver::Dict{UInt32,Channel}
    processor::Task

    function ClientChannel(host::AbstractString, port::Integer)
        socket = connect(host, port)
        connection = new_connection(socket, isclient = true)
        authority = "$host:$port"
        receiver = Dict{UInt32,Channel}()

        processor = @schedule begin
            while true
                if connection.closed
                    if !isempty(receiver)
                        throw(ProtocolError("Connection lost"))
                    end
                    break
                end

                evt = take_evt!(connection)

                if !haskey(receiver, evt.stream_identifier)
                    throw(ProtocolError("Stream removed"))
                end

                if !(evt isa EvtRecvHeaders || evt isa EvtRecvData)
                    throw(ProtocolError("Unexpected event type $(typeof(evt))"))
                end

                put!(receiver[evt.stream_identifier], evt)
            end
        end

        new(host, port, socket, connection, authority, receiver, processor)
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

function isopen(channel::ClientChannel, stream_id::UInt32)
    haskey(channel.receiver, stream_id)
end

function put!(channel::ClientChannel, request::ServiceRequest,
              stream_id::UInt32, isend::Bool)
    if !isopen(channel, stream_id)
        throw(ProtocolError("Stream removed"))
    end
    put_act!(channel.connection, ActSendHeaders(stream_id, request, isend))
end

function put!(channel::ClientChannel, message::ProtoType,
              stream_id::UInt32, isend::Bool)
    if !isopen(channel, stream_id)
        throw(ProtocolError("Stream removed"))
    end

    put_act!(channel.connection, ActSendData(stream_id, pack(message), isend))
end

################################################################################
# Stream, manage GRPC call connection
################################################################################

function process_headers(headers::Headers)
    @assert headers[":status"] == "200"
    @assert headers["content-type"] in CONTENT_TYPES
    status = Status(parse(Int, get(headers, "grpc-status", string(Int(OK)))))
    if status != OK
        status_message = get(headers, "grpc-message", "")
        throw(GRPCError(status, status_message))
    end
end

function process_trailers(headers::Headers)
    status = Status(parse(Int, headers["grpc-status"]))
    if status != OK
        status_message = get(headers_map, "grpc-message", "")
        throw(GRPCError(status, status_message))
    end
end

mutable struct ClientStream{T,U}
    channel::ClientChannel
    method_name::AbstractString
    metadata::Nullable{Metadata}
    deadline::Nullable{Deadline}

    stream_id::UInt32
    receiver::Channel

    requests::Channel{Tuple{T,Bool}}
    request_processor::Task

    responses::Channel{U}
    response_processor::Task

    buffer::Vector{UInt8}
    headers::Nullable{Headers}
    trailers::Nullable{Headers}

    function ClientStream{RequestType, ResponseType}(channel::ClientChannel,
                                                     method_name::AbstractString;
                                                     metadata = Nullable{Metadata}(),
                                                     deadline = Nullable{Deadline}()) where
        {RequestType,ResponseType}

        ret = new{RequestType,ResponseType}()

        ret.channel = channel
        ret.method_name = method_name
        ret.metadata = metadata
        ret.deadline = deadline

        ret.stream_id = next_free_stream_identifier(ret.channel.connection)

        ret.receiver = Channel(32)
        bind(ret.receiver, ret.channel.processor)
        ret.channel.receiver[ret.stream_id] = ret.receiver

        ret.requests = Channel{Tuple{RequestType,Bool}}(32)

        ret.request_processor = @schedule begin
            request = ServiceRequest("POST", "http", method_name,
                                     ret.channel.authority,
                                     content_type = CONTENT_TYPE,
                                     user_agent = "grpc-julia-GRPC",
                                     metadata = metadata, deadline = deadline)
            put!(ret.channel, request, ret.stream_id, false)
            for (req, isend) in ret.requests
                put!(ret.channel, req, ret.stream_id, isend)
                yield()
            end
        end

        bind(ret.requests, ret.request_processor)

        ret.responses = Channel{ResponseType}(32)
        ret.buffer = Vector{UInt8}()

        ret.response_processor = @schedule begin
            wait(ret.receiver)
            evt = take!(ret.receiver)

            if !(evt isa EvtRecvHeaders)
                throw(ProtocolError("Failed to receive initial metadata"))
            end

            ret.headers = evt.headers
            process_headers(evt.headers)

            if evt.is_end_stream
                throw(ProtocolError("Failed to receive message data"))
            end

            while true
                wait(ret.receiver)
                evt = take!(ret.receiver)

                if evt isa EvtRecvHeaders
                    ret.trailers = evt.headers
                    process_trailers(evt.headers)

                    if !evt.is_end_stream
                        throw(ProtocolError("Failed to receive end of stream"))
                    end

                    break
                elseif evt.is_end_stream
                    throw(ProtocolError("Failed to receive trailing metadata"))
                end

                # process message data

                append!(ret.buffer, evt.data)
                if length(ret.buffer) < 5
                    throw(ProtocolError("Incomplete message header"))
                end

                len = ntoh(read(IOBuffer(ret.buffer[2:5]), UInt32)) + 5
                while length(ret.buffer) < len
                    wait(ret.receiver)
                    evt = take!(ret.receiver)
                    if !(evt isa EvtRecvData)
                        throw(ProtocolError("Incomplete message data"))
                    end
                    append!(ret.buffer, evt.data)
                end

                if length(ret.buffer) == len
                    put!(ret.responses, unpack(ret.buffer, ResponseType))
                    resize!(ret.buffer, 0)
                else
                    put!(ret.responses, unpack(ret.buffer[1:len], ResponseType))
                    ret.buffer = ret.buffer[(len + 1):end]
                end
            end
        end
        bind(ret.responses, ret.response_processor)

        ret
    end
end

function put!(stream::ClientStream{RequestType,ResponseType}, request) where
    {RequestType, ResponseType}
    put!(stream.requests, request)
    if request[2]
        close(stream.requests)
    end
end

function take!(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    take!(stream.responses)
end

function fetch(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    fetch(stream.responses)
end

function collect(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    collect(stream.responses)
end

function start(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    start(stream.resopnse)
end

function next(stream::ClientStream{RequestType,ResponseType}, state) where
    {RequestType, ResponseType}
    next(stream.resopnse, state)
end

function done(stream::ClientStream{RequestType,ResponseType}, state) where
    {RequestType, ResponseType}
    done(stream.resopnse, state)
end
