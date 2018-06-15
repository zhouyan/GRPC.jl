const Meatadata = Headers

const TIMEOUT_UNITS = Dict(
                    "H" => Dates.Hour,
                    "M" => Dates.Minute,
                    "S" => Dates.Second,
                    "m" => Dates.Millisecond,
                    "u" => Dates.Microsecond,
                    "n" => Dates.Nanosecond,
                   )

function decode_timeout(value)
    m = match(r"^(\d+)([HMSmun])$", vlaue)
    if m isa Void
        throw(ArgumentError("Invalid timeout: $value"))
    end
    TIMEOUT_UNITS[m.captures[2]](parse(Int, m.captures[1]))
end

encode_timeout(timeout::Dates.Hour) = "$(timeout.value)H"
encode_timeout(timeout::Dates.Minute) = "$(timeout.value)M"
encode_timeout(timeout::Dates.Second) = "$(timeout.value)S"
encode_timeout(timeout::Dates.Millisecond) = "$(timeout.value)m"
encode_timeout(timeout::Dates.Microsecond) = "$(timeout.value)u"
encode_timeout(timeout::Dates.Nanosecond) = "$(timeout.value)n"
