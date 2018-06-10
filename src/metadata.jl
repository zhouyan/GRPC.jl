import Base: <, ==, convert
import HttpCommon: Headers

struct Metadata
    headers::Headers
    body::Vector{Tuple{AbstractString,AbstractString}}

    function Metadata(hdrs::Headers)
        ret = new(Headers("content-type" => "te"), Tuple{AbstractString,Any}[])
        for (k, v) in hdrs
            if !isempty(k) && k[1] != ':' && k != "content-type"
                push!(ret.body, (k, v))
            end
        end
        ret
    end
end

Metadata() = Metadata(Headers())

function getall(metadata::Metadata, key::AbstractString)
    ret = AbstractString[]
    for (k, v) in metadata.body
        if k == key
            push!(ret, v)
        end
    end
    ret
end

const _UNITS = Dict(
                    "H" => 60 * 60 * 1e9,
                    "M" => 60 * 1e9,
                    "S" => 1e9,
                    "m" => 1e6,
                    "u" => 1e3,
                    "n" => 1.0,
                   )

function decode_timeout(value)
    m = match(r"^(\d+)([HMSmun])$", vlaue)
    if m isa Void
        throw(ArgumentError("Invalid timeout: $value"))
    end
    trunc(UInt64, parse(Int, m.captures[1]) * _UNITS[m.captures[2]])
end

encode_timeout(timeout::Dates.Hour) = "$(timeout.value)H"
encode_timeout(timeout::Dates.Minute) = "$(timeout.value)M"
encode_timeout(timeout::Dates.Second) = "$(timeout.value)S"
encode_timeout(timeout::Dates.Millisecond) = "$(timeout.value)m"
encode_timeout(timeout::Dates.Microsecond) = "$(timeout.value)u"
encode_timeout(timeout::Dates.Nanosecond) = "$(timeout.value)n"

function econde_timeout(timeout::Number)
    if timeout > 10
        econde_timetout(Dates.Second(trunc(Int, timeout)))
    elseif timeout > 1e-2
        econde_timetout(Dates.Millisecond(trunc(Int, timeout * 1e3)))
    elseif timeout > 1e-5
        econde_timetout(Dates.Microsecond(trunc(Int, timeout * 1e6)))
    else
        econde_timetout(Dates.Nanosecond(trunc(Int, timeout * 1e9)))
    end
end

struct Deadline
    timestamp::UInt64

    Deadline(::Void) = new(0)
    Deadline(timeout::Integer) = new(time_ns() + timeout)
end

function Deadline(metadata::Metadata)
    t = getall(metadata, "grpc-timeout")
    if isempty(t)
        Detaline(nothing)
    end
    Deadline(maximum(parse.(Int, t)))
end

<(d1::Deadline, d2::Deadline) = d1.timestamp < d2.timestamp
==(d1::Deadline, d2::Deadline) = d1.timestamp == d2.timestamp

function time_remaining(dt::Deadline)
    t = time_ns()
    t >= dt.timestamp ? 0.0 : (dt.timestamp - t) * 1e-9
end

struct Request
    method::AbstractString
    scheme::AbstractString
    path::AbstractString
    authority::AbstractString
    content_type::Nullable{AbstractString}
    message_type::Nullable{AbstractString}
    message_encoding::Nullable{AbstractString}
    message_accept_encoding::Nullable{AbstractString}
    user_agent::Nullable{AbstractString}
    metadata::Nullable{Metadata}
    deadline::Nullable{Deadline}

    function Request(method::AbstractString, scheme::AbstractString,
                     path::AbstractString, authority::AbstractString;
                     content_type = Nullable{AbstractString}(),
                     message_type = Nullable{AbstractString}(),
                     message_econding = Nullable{AbstractString}(),
                     message_accept_encoding = Nullable{AbstractString}(),
                     user_agent = Nullable{AbstractString}(),
                     metadata = Nullable{Metadata}(),
                     deadline = Nullable{Deadline}())
        new(method, scheme, path, authority, content_type,
            message_type, message_econding, message_accept_encoding,
            user_agent, metadata, deadline)
    end
end

function convert(::Type{Headers}, request::Request)
    ret = Headers(
                  ":method" => request.method,
                  ":scheme" => request.scheme,
                  ":path" => request.path,
                  ":authority" => request.authority,
                 )

    if !isnull(request.deadline)
        timeout = time_remaining(get(request.deadline))
        ret["grpc-timetout"] = encode_timeout(timeout)
    end

    if !isnull(request.content_type)
        ret["content-type"] = get(request.content_type)
    end

    if !isnull(request.message_type)
        ret["grpc-message-type"] = get(request.message_type)
    end

    if !isnull(request.message_encoding)
        ret["grpc-encoding"] = get(request.message_encoding)
    end

    if !isnull(request.message_accept_encoding)
        ret["grpc-accept-encoding"] = get(request.message_accept_encoding)
    end

    if !isnull(request.user_agent)
        ret["user-agent"] = get(request.user_agent)
    end

    if !isnull(request.metadata)
        for (k, v) in get(request.metadata).body
            if k != "grpc-timeout"
                ret[k] = v
            end
        end
    end

    ret
end
