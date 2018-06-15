struct ClientRequest
    method::AbstractString
    scheme::AbstractString
    path::AbstractString
    authority::AbstractString
    content_type::Nullable{AbstractString}
    grpc_message_type::Nullable{AbstractString}
    grpc_encoding::Nullable{AbstractString}
    grpc_accept_encoding::Nullable{AbstractString}
    user_agent::Nullable{AbstractString}
    metadata::Nullable{Headers}
    timeout::Nullable{Dates.Period}

    function ClientRequest(method::AbstractString, scheme::AbstractString,
                           path::AbstractString, authority::AbstractString;
                           content_type = Nullable(CONTENT_TYPE),
                           grpc_message_type = Nullable{AbstractString}(),
                           grpc_encoding = Nullable{AbstractString}(),
                           grpc_accept_encoding = Nullable{AbstractString}(),
                           user_agent = Nullable("GRPC.jl"),
                           metadata = Nullable{Headers}(),
                           timeout = Nullable{Dates.Period}())
        new(method, scheme, path, authority, content_type,
            grpc_message_type, grpc_encoding, grpc_accept_encoding,
            user_agent, metadata, timeout)
    end
end

function convert(::Type{Headers}, request::ClientRequest)
    ret = Headers(
                  ":method" => request.method,
                  ":scheme" => request.scheme,
                  ":path" => request.path,
                  ":authority" => request.authority,
                  "te" => "trailers",
                 )

    if !isnull(request.timeout)
        ret["grpc-timetout"] = encode_timeout(get(timeout))
    end

    if !isnull(request.content_type)
        ret["content-type"] = get(request.content_type)
    end

    if !isnull(request.grpc_message_type)
        ret["grpc-message-type"] = get(request.grpc_message_type)
    end

    if !isnull(request.grpc_encoding)
        ret["grpc-encoding"] = get(request.grpc_encoding)
    end

    if !isnull(request.grpc_accept_encoding)
        ret["grpc-accept-encoding"] = get(request.grpc_accept_encoding)
    end

    if !isnull(request.user_agent)
        ret["user-agent"] = get(request.user_agent)
    end

    if !isnull(request.metadata)
        for (k, v) in get(request.metadata)
            if k != "grpc-timeout"
                ret[k] = v
            end
        end
    end

    ret
end
