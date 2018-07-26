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
        status_message = get(headers, "grpc-message", "")
        throw(GRPCError(status, status_message))
    end
end

mutable struct ClientStream{T,U}
    channel::ClientChannel
    path::AbstractString
    metadata::Nullable{Headers}
    timeout::Nullable{Dates.Period}
    headers::Nullable{Headers}
    trailers::Nullable{Headers}

    stream_id::UInt32
    receiver::Channel

    requests::Channel{Tuple{T,Bool}}
    request_processor::Task

    responses::Channel{U}
    response_processor::Task

    function ClientStream{RequestType, ResponseType}(channel::ClientChannel,
                                                     path::AbstractString;
                                                     metadata = Nullable{Headers}(),
                                                     timeout = Nullable{Dates.Period}()) where
        {RequestType,ResponseType}

        ret = new{RequestType,ResponseType}()

        ret.channel = channel
        ret.path = path
        ret.metadata = metadata
        ret.timeout = timeout
        ret.headers = Nullable{Headers}()
        ret.trailers = Nullable{Headers}()

        request = ClientRequest("POST", "http", path, ret.channel.authority,
                                metadata = metadata, timeout = timeout)

        ret.stream_id, ret.receiver = open(ret.channel, request)

        ret.requests = Channel{Tuple{RequestType,Bool}}(32)

        ret.request_processor = @schedule begin
            for (req, isend) in ret.requests
                put!(ret.channel, req, ret.stream_id, isend)
            end
        end

        bind(ret.requests, ret.request_processor)

        ret.responses = Channel{ResponseType}(32)

        ret.response_processor = @schedule begin
            # TODO deadline

            evt = take!(ret.receiver)

            if !(evt isa EvtRecvHeaders)
                throw(ProtocolError("Headers: no initial metadata"))
            end

            ret.headers = evt.headers
            process_headers(evt.headers)

            if evt.is_end_stream
                throw(ProtocolError("Response: no message data"))
            end

            buffer = Vector{UInt8}()

            while true
                if length(buffer) < 5
                    evt = take!(ret.receiver)

                    if evt isa EvtRecvHeaders
                        ret.trailers = evt.headers
                        process_trailers(evt.headers)

                        if !evt.is_end_stream
                            throw(ProtocolError("Trailers: stream not ended"))
                        end

                        if !isempty(buffer)
                            throw(ProtocolError("Trailers: unprocessed data"))
                        end

                        break
                    elseif evt.is_end_stream
                        throw(ProtocolError("Trailers: no trailing metadata"))
                    end

                    append!(buffer, evt.data)
                    if length(buffer) < 5
                        throw(ProtocolError("Response: incomplete message header"))
                    end
                end

                len = ntoh(read(IOBuffer(buffer[2:5]), UInt32)) + 5
                while length(buffer) < len
                    evt = take!(ret.receiver)
                    if !(evt isa EvtRecvData)
                        throw(ProtocolError("Response: incomplete message data"))
                    end
                    append!(buffer, evt.data)
                end

                if length(buffer) == len
                    put!(ret.responses, unpack(buffer, ResponseType))
                    resize!(buffer, 0)
                else
                    # TODO use splice view to reduce allocation
                    put!(ret.responses, unpack(buffer[1:len], ResponseType))
                    buffer = buffer[(len + 1):end]
                end
            end
        end

        bind(ret.responses, ret.response_processor)

        finalizer(ret, obj -> close(obj.channel, obj.stream_id))

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

function wait(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    wait(stream.responses)
end

function close(stream::ClientStream{RequestType,ResponseType},
               quick_exit = true) where {RequestType, ResponseType}
    close(stream.requests)
    if quick_exit
        close(stream.receiver)
        wait(response_processor)
    end
end

function isopen(stream::ClientStream{RequestType,ResponseType}) where
    {RequestType, ResponseType}
    isopen(stream.requests)
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
    start(stream.responses)
end

function next(stream::ClientStream{RequestType,ResponseType}, state) where
    {RequestType, ResponseType}
    next(stream.responses, state)
end

function done(stream::ClientStream{RequestType,ResponseType}, state) where
    {RequestType, ResponseType}
    done(stream.responses, state)
end
