mutable struct ClientChannel
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

function ClientChannel()
    ClientChannel("127.0.0.1", 50051)
end

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

function open(channel::ClientChannel, request::ClientRequest)
    stream_id = UInt32(next_free_stream_identifier(channel.connection))

    if haskey(channel.receiver, stream_id)
        throw(ProtocolError("Duplicate stream id"))
    end

    if channel.connection.last_stream_identifier <= stream_id
        channel.connection.last_stream_identifier = stream_id
    end

    channel.receiver[stream_id] = Channel(32)
    put_act!(channel.connection, ActSendHeaders(stream_id, request, false))

    stream_id, channel.receiver[stream_id]
end

function close(channel::ClientChannel, stream_id::UInt32)
    if haskey(channel.receiver, stream_id)
        delete!(channel.receiver, stream_id)
    end
end

function isopen(channel::ClientChannel, stream_id::UInt32)
    haskey(channel.receiver, stream_id)
end

function put!(channel::ClientChannel, message::ProtoType,
              stream_id::UInt32, isend::Bool)
    if !isopen(channel, stream_id)
        throw(ProtocolError("Stream removed"))
    end
    put_act!(channel.connection, ActSendData(stream_id, pack(message), isend))
end
