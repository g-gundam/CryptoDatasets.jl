module CryptoDatasets
using JSON3
using CSV
using Dates
using NanoDates
using DataFrames
using DataFramesMeta
using Printf

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
    m = match(r"(\d{4})-(\d{2})-(\d{2})", ds)
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
function dataset(exchange, market; srctf="1m", datadir="./data", span=missing, tf::Period=Minute(1))
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

function _last_csv(outdir)
    cfs = readdir(outdir)
    if length(cfs) == 0
        missing
    else
        cfs[end]
    end
end

function _unix_ms(filename)
    ms = replace(filename, ".csv" => "") |> DateTime |> datetime2unix |> ts -> ts * 1000
    convert(Int64, ms)
end

"""
_import_json!(exchange, market, timeframe)

Import data from a previous project of mine that stored this info in JSON files.
"""
function _import_json!(exchange, market; tf="1m", srcdir="", datadir="./data", sincelast=true)
    dir = joinpath(srcdir, exchange, market, tf)
    jfs = readdir(dir; join=true) # jfs means JSON files

    write = []
    outdir = joinpath(datadir, exchange, market, tf)
    mkpath(outdir)

    # fast forward to where we left off.
    start = 1
    _skip_first_write = false
    if sincelast
        csv_name = _last_csv(outdir)
        if ismissing(csv_name)
            start = 1
        else
            ms = _unix_ms(csv_name)
            i = findfirst(jfs) do f
                bf = basename(f)
                n = parse(Int64, replace(bf, ".json" => ""))
                n > ms
            end
            if !isnothing(i)
                start = i - 1
                _skip_first_write = true
            end
        end
    end

    # I want to fill a fixed-size bin with candles until it's full.
    # Then I want to write the bin to a file.
    # Then create a new empty fixed-size bin and repeat the process until
    #   all the candles are handled.
    bin = missing
    current_day = missing
    next_day = missing
    for jf in jfs[start:end]
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
                push!(write, bin)
                outfile = outdir * "/" * NanoDates.format(current_day, "yyyy-mm-dd") * ".csv"
                bindf = bin |> DataFrame
                if _skip_first_write
                    @debug "Skipping first write"
                    _skip_first_write = false
                else
                    @info "Write $current_day : $(length(bin)) candles"
                    CSV.write(outfile, bindf)
                end
                # create new bin
                @debug "New Bin"
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
        outfile = outdir * "/" * NanoDates.format(current_day, "yyyy-mm-dd") * ".csv"
        CSV.write(outfile, bin |> DataFrame)
    end
    write
end

const Time0 = DateTime(1970, 1, 1)

_millis2nanodate(millis::Millisecond) = Time0 + millis

Base.convert(::Type{NanoDate}, ts::UInt64) = _millis2nanodate(Millisecond(ts))

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
using CryptoDatasets: _import_json!, dataset
using CSV
using Dates
using NanoDates
using JSON3
using DataFrames
using DataFramesMeta

# load btcusd
btcusd = dataset("bybit", "BTCUSD", span=Date("2023-03-01"):Date("2023-03-07"))

# How to do candle aggregation the Julia+DataFrame way
btcusd30m = @chain btcusd begin
    @transform(:ts2 = floor.(:ts, Minute(30)))
    groupby(:ts2)
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

# How to do aggregation in general
df = DataFrame(k=0:9, v=11:20)
df2 = @chain df begin
    @transform(:gb = floor.(:k / 5))
    groupby(:gb)
    @combine(:s = sum(:v))
    @select(:s)
end

# How to import my old JSON data into CryptoDatasets
srcdir = "$(ENV["HOME"])/src/git.coom.tech/gg1234/ta/data"
_import_json!("bitmex", "XBTUSD"; srcdir=srcdir)

"""
