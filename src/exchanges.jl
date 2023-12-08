using URIs
using HTTP
using JSON3
using DataStructures

abstract type AbstractExchange end
# Operations
# get_markets(::AbstractExchange)
# get_candles(::AbstractExchange; start, stop, limit)

struct BitStamp <: AbstractExchange
    base_url::String
end

function get_markets(bitstamp::BitStamp)
    market_url = bitstamp.base_url * "/ticker/"
    res = HTTP.get(market_url)
    json = JSON3.read(res.body)
    return map(r -> r.pair, json)
end

function get_candles(bitstamp::BitStamp, market; start::DateTime, stop::DateTime, limit::Integer=10)
    mark2 = replace(market, r"\W" => s"") |> lowercase
    q2 = OrderedDict(
        "step" => 60,
        "start" => round(Int, datetime2unix(start)),
        "end" => round(Int, datetime2unix(stop)),
        "limit" => limit
    )
    ohlc_url = bitstamp.base_url * "/ohlc/" * mark2 * "/"
    uri = URI(ohlc_url, query=q2)
    @warn uri
    res = HTTP.get(uri)
    json = JSON3.read(res.body)
    return json[:data][:ohlc]
end

struct PancakeSwap <: AbstractExchange
end
