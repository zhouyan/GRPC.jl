import ProtoBuf: readproto, writeproto
import ProtoBuf.Gen: codegen
import ProtoBuf.GoogleProtoBuf: FileOptions
import ProtoBuf.GoogleProtoBufCompiler: CodeGeneratorRequest
import ProtoBuf.GoogleProtoBufCompiler: CodeGeneratorResponse
import ProtoBuf.GoogleProtoBufCompiler: CodeGeneratorResponse_File

################################################################################
# ClientStub a container of channel
################################################################################

abstract type ClientStub end

################################################################################
# Client method
################################################################################

function method_gen_unary_unary(stub, name, path, request_type, response_type)
    """
    function $name(stub::$stub; kwargs...)
        GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
    end

    function $name(stub::$stub,
        request::$request_type;
        kwargs...)
        ret = GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
        put!(ret, (request, true))
        ret
    end
    """
end

function method_gen_unary_stream(stub, name, path, request_type, response_type)
    """
    function $name(stub::$stub; kwargs...)
        GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
    end

    function $name(stub::$stub,
        request::$request_type;
        kwargs...)
        ret = GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
        put!(ret, (request, true))
        ret
    end
    """
end

function method_gen_stream_unary(stub, name, path, request_type, response_type)
    """
    function $name(stub::$stub; kwargs...)
        GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
    end
    """
end

function method_gen_stream_stream(stub, name, path, request_type, response_type)
    """
    function $name(stub::$stub; kwargs...)
        GRPC.ClientStream{
            $request_type,
            $response_type
            }(
            stub.channel,
            "$path";
            kwargs...)
    end
    """
end

function method_gen(stub_name, service_name, name,
                    client_stream, server_stream, request_type, response_type)
    path = "/$service_name/$name"
    stub = "$(stub_name)Stub"
    if !client_stream && !server_stream
        method_gen_unary_unary(stub, name, path, request_type, response_type)
    elseif !client_stream && server_stream
        method_gen_unary_stream(stub, name, path, request_type, response_type)
    elseif client_stream && !server_stream
        method_gen_stream_unary(stub, name, path, request_type, response_type)
    else
        method_gen_stream_stream(stub, name, path, request_type, response_type)
    end
end

function service_gen(proto_file, package, imports, services)
    lines = String[]
    push!(lines, "#Generated by the Protocol Buffers compiler. DO NOT EDIT!")
    push!(lines, "# source: $proto_file")
    push!(lines, "# plugin: julia_grpc")

    push!(lines, "")
    push!(lines, "import GRPC")
    for mod in imports
        push!(lines, "import $mod")
    end

    push!(lines, "")
    for (name, methods) in services
        if isempty(package)
            service_name = name
        else
            service_name = "$package.$(name)"
        end

        push!(lines, """
struct $(name)Stub <: GRPC.ClientStub
    channel::GRPC.ClientChannel
end
""")

        for method in methods
            push!(lines, method_gen(name, service_name, method...))
        end
    end

    join(lines, '\n')
end

function _proto_typename(protofile, message_type)
    if isempty(protofile.package)
        ".$(message_type.name)"
    else
        ".$(protofile.package).$(message_type.name)"
    end
end

function _julia_typename(protofile, message_type)
    msgname = replace(message_type.name, ".", "_")

    if isempty(protofile.package)
        ret = msgname
    else
        ret = "$(protofile.package).$msgname"
    end

    if ismatch(r"^google\.protobuf\.", ret)
        ret = "ProtoBuf.$ret"
    end

    ret
end

function _get_proto(request, name)
    for f in request.proto_file
        if f.name == name
            return f
        end
    end
end

function _depname(request, name)
    # TODO module postfix
    proto_file = _get_proto(request, name)
    if !isempty(proto_file.package)
        mod = split(proto_file.package, ".")[1]
        if mod == "google"
            mod = "ProtoBuf"
        end
        return mod
    else
        error("depenency that's not in a package is not handled yet")
    end
end

function codegen(request::CodeGeneratorRequest)::CodeGeneratorResponse
    types = Dict()
    for pf in request.proto_file
        for mt in pf.message_type
            types[_proto_typename(pf, mt)] = _julia_typename(pf, mt)
        end
    end

    ret = CodeGeneratorResponse(file = CodeGeneratorResponse_File[])

    for file_to_generate in request.file_to_generate
        proto_file = _get_proto(request, file_to_generate)

        imports = String[]
        for dep in proto_file.dependency
            push!(imports, _depname(request, dep))
        end
        push!(imports, _depname(request, file_to_generate))
        imports = unique(imports)

        services = []
        for service in proto_file.service
            methods = []
            for method in service.method
                push!(methods, (
                                method.name,
                                method.client_streaming,
                                method.server_streaming,
                                types[method.input_type],
                                types[method.output_type],
                               ))
            end
            push!(services, (service.name, methods))
        end

        name = replace(file_to_generate, r"(.*)\.proto", s"\1_grpc.jl")
        content = service_gen(proto_file.name, proto_file.package,
                              imports, services)
        file = CodeGeneratorResponse_File(name = name, content = content)
        push!(ret.file, file)
    end

    ret
end

function codegen()
    try
        request = readproto(STDIN, CodeGeneratorRequest())
        writeproto(STDOUT, codegen(request))
    catch ex
        println(STDERR, "Exception while generating Julia code")
        rethrow()
    end
end
