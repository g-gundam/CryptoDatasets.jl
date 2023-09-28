module CryptoDatasets
using NanoDates
using JSON3
using CSV
using Dates
using NanoDates
using DataFrames

"""
A candle containing a timestamp, OHLCV values,
and an optional `v2` for weird exchanges that return 2 volumes.
"""
struct Candle{TSType}
    ts::TSType
    o::Union{Float64,Missing}
    h::Union{Float64,Missing}
    l::Union{Float64,Missing}
    c::Union{Float64,Missing}
    v::Float64
    v2::Union{Float64,Missing} # only bitget uses this
end

"""
dataset(exchange, market) => Vector{Any}

Return OHLC candles for the given exchange and market.
"""
function dataset(exchange, market)
    [42, exchange, market]
end

"""
_sanitize()

Take a row from the JSON import and make it suitable for writing out as a
CSV row.

# Examples
```jldoctest
julia> CryptoDatasets._sanitize([1636675140000, 1, 2, 0.5, 1, 1])
7-element Vector{Any}:
 1.63667514e12
 1.0
 2.0
 0.5
 1.0
 1.0
  missing
```
"""
function _sanitize(c)
    c2 = []
    for i in c
        if typeof(i) == Nothing
            push!(c2, missing)
        else
            push!(c2, i)
        end
    end
    if length(c) == 6
        push!(c2, missing)
    end
    c2
end

function import_json!(exchange, market, timeframe; srcdir="", datadir="./data")
    dir = joinpath(srcdir, exchange, market, timeframe)
    jfs = readdir(dir; join=true)
    write = []
    outdir = joinpath(datadir, exchange, market, timeframe)
    mkpath(outdir)

    # I want to fill a fixed-size bin with candles until it's full.
    # Then I want to write the bin to a file.
    # Then create a new empty fixed-size bin and repeat the process until
    #   all the candles are handled.
    bin = missing
    current_day = missing
    next_day = missing
    for jf in jfs
        candles = jf |> read |> JSON3.read
        for c in candles
            nd = _millis2nanodate(Millisecond(c[1]))
            if ismissing(bin)
                # initial bin
                c2 = _sanitize(c)
                cc = Candle{UInt64}(c2...)
                @debug "Initial", cc.ts |> Millisecond |> _millis2nanodate
                bin = [cc]
                current_day = floor(nd, Day)
                next_day = current_day + Day(1)
                continue
            elseif floor(nd, Day) == next_day
                # write out bin
                @info "Pretend to Write $current_day : $(length(bin)) candles"
                push!(write, bin)
                outfile = outdir * "/" * NanoDates.format(current_day, "yyyymmdd") * ".csv"
                bindf = bin |> DataFrame
                CSV.write(outfile, bindf)
                # create new bin
                @debug "Write"
                c2 = _sanitize(c)
                cc = Candle{UInt64}(c2...)
                bin = [cc]
                current_day = floor(nd, Day)
                next_day = current_day + Day(1)
                continue
            else
                # push to existing bin
                c2 = _sanitize(c)
                cc = Candle{UInt64}(c2...)
                #@debug "Existing", cc.ts |> Millisecond |> _millis2nanodate
                push!(bin, cc)
            end
        end
    end
    if length(bin) > 0
        @info "Last Write", current_day
        push!(write, bin)
        outfile = outdir * "/" * NanoDates.format(current_day, "yyyymmdd") * ".csv"
        CSV.write(outfile, bin |> DataFrame)
    end
    write
end

function aggregate(tf::Period, ca::Candle, previous::Union{Candle,Missing}=missing)
    if ismissing(previous)
        return ca
    end
    C = typeof(ca)
    o = ca.o
    h = ca.h < previous.h ? previous.h : ca.h
    l = ca.l > previous.l ? previous.l : ca.l
    c = previous.c
    v = previous.v
    v2 = previous.v2
    C(floor(DateTime(ca.ts), tf), o, h, l, c, v, v2)
end

function cand(c::DataFrameRow)
    nd = _millis2nanodate(Millisecond(c.ts))
    Candle{NanoDate}(nd, c.o, c.h, c.l, c.c, c.v, c.v2)
end

# This should be:
# dataset("bitmex", "XBTUSD"; tf=Hour(4), last=Day(7))
function a5m(ca)
    reduce(ca; init=[]) do m, a
        @debug typeof(m), typeof(a)
        if length(m) == 0
            c = aggregate(Minute(5), a)
            return [c]
        else
            if floor(DateTime(a.ts), Minute(5)) != m[end].ts
                c = aggregate(Minute(5), a)
                push!(m, c)
            else
                c = aggregate(Minute(5), a, m[end])
                m[end] = c
            end
            return m
        end
    end
end

const Time0 = DateTime(1970, 1, 1)

_millis2nanodate(millis::Millisecond) = Time0 + millis

end


# This is a hack I learned from the forum that allows
# LSP to recognize the current package when editing scripts.
# https://discourse.julialang.org/t/lsp-missing-reference-woes/98231/16
# macro ignore(args...) end
# @ignore include("../bin/fetch.jl")
# @ignore include("../bin/nanotest.jl")

"""
# REPL init

using CryptoDatasets
using CryptoDatasets: Candle
using CryptoDatasets: import_json!, aggregate
using Dates
using NanoDates
using CSV
using JSON3
using DataFrames
using TimeSeries

srcdir = "$(ENV["HOME"])/src/git.coom.tech/gg1234/ta/data"
import_json!("bitmex", "XBTUSD", "1m", srcdir=srcdir)

cs = CSV.read("./data/bybit/BTCUSD/1m/20211111.csv", DataFrame; types=Dict("ts" => UInt64))
ca = eachrow(first(cs, 10)) .|> CryptoDatasets.cand
CryptoDatasets.a5m(ca)

"""
