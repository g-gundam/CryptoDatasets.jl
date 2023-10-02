module CryptoDatasets
using NanoDates
using JSON3
using CSV
using Dates
using NanoDates
using DataFrames
using DataFramesMeta

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

function _filename_to_date(f)
    ds = replace(basename(f), ".csv" => "")
    m = match(r"(\d{4})(\d{2})(\d{2})", ds)
    Date(parse.(Int32, m.captures)...)
end

# date to index in span
function _d2i(d::Date, cfs)
    a = _filename_to_date(first(cfs))
    b = _filename_to_date(last(cfs))
    if a <= d <= b
        diff = d - a
        return diff.value + 1
    else
        missing
    end
end

"""
dataset(exchange, market; tf::Period, dates) => Vector{Any}

Return OHLC candles for the given exchange and market.
"""
function dataset(exchange, market; srctf="1m", datadir="./data", span=missing, tf::Period=missing)
    indir = joinpath(datadir, exchange, market, srctf)
    cfs = readdir(indir; join=true)
    if !ismissing(span)
        if typeof(span) <: UnitRange
            cfs = cfs[span]
        elseif typeof(span) <: StepRange
            # convert span to UnitRange
            a = _d2i(first(span), cfs)
            b = _d2i(last(span), cfs)
            cfs = cfs[range(a, b)]
        end
    end
    res = missing
    for cf in cfs
        csv = CSV.read(cf, DataFrame; types=Dict(:ts => UInt64))
        csv[!, :ts] = convert.(NanoDate, csv[!, :ts])
        if ismissing(res)
            res = csv
        else
            append!(res, csv)
        end
    end

    # Do optional timeframe summarization
    if ismissing(tf)
        return res
    else
        return @chain res begin
            @transform(:ts2 = floor.(:ts, tf))
            groupby(:ts2) # LSP doesn't know the @chain macro is doing magic.
            @combine begin
                :o = first(:o)
                :h = maximum(:h)
                :l = minimum(:l)
                :c = last(:c)
                :v = sum(:v)
                :v2 = sum(:v2)
            end
            @select(:ts = :ts2, :o, :h, :l, :c, :v, :v2)
        end
    end
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

"""
import_json!(exchange, market, timeframe)

Import data from a previous project of mine that stored this info in JSON files.
"""
function import_json!(exchange, market; tf="1m", srcdir="", datadir="./data", sincelast=true)
    dir = joinpath(srcdir, exchange, market, tf)
    jfs = readdir(dir; join=true)
    write = []
    outdir = joinpath(datadir, exchange, market, tf)
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

const Time0 = DateTime(1970, 1, 1)

_millis2nanodate(millis::Millisecond) = Time0 + millis

_unix_epoch_ms =
    Dates.value(unix2datetime(0)) +
    abs(Dates.value(Dates.epochms2datetime(0)))
function _unixms2datetime(ms)
    Dates.epochms2datetime(_unix_epoch_ms + ms)
end

Base.convert(::Type{NanoDate}, ts::UInt64) = _millis2nanodate(Millisecond(ts))

end

x = zip(1:10, 11:20)
y = foldl(x; init=[]) do m, a
    if length(m) == 0
        # first iteration
        return [a[2]]
    else
        if (a[1] - 1) % 5 == 0
            # introduce a new value after every 5 items in x
            push!(m, a[2])
        else
            # sum
            m[end] += a[2]
        end
        return m
    end
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
using CryptoDatasets: import_json!, _ohlc, dataset
using CSV
using Dates
using NanoDates
using JSON3
using TimeSeries
using DataFrames
using DataFramesMeta

btcusd30m = @chain btcusd begin
    @transform(:ts2 = floor.(:ts, Minute(30)))
    groupby(:ts2)
    @combine begin
        :o = first(:o)
        :h = maximum(:h)
        :l = minimum(:l)
        :c = last(:c)
        :v = last(:v)
        :v2 = last(:v2)
    end
    @select(:ts = :ts2, :o, :h, :l, :c, :v, :v2)
end

df = DataFrame(k=0:9, v=11:20)
df2 = @chain df begin
    @transform(:gb = floor.(:k / 5))
    groupby(:gb)
    @combine(:s = sum(:v))
    @select(:s)
end

srcdir = "$(ENV["HOME"])/src/git.coom.tech/gg1234/ta/data"
import_json!("bitmex", "XBTUSD"; srcdir=srcdir)

cs = CSV.read("./data/bybit/BTCUSD/1m/20211111.csv", DataFrame; types=Dict("ts" => UInt64))
ca = eachrow(first(cs, 60)) .|> CryptoDatasets.cand
CryptoDatasets.a5m(ca)

"""
