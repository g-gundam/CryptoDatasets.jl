module CryptoDatasets
using JSON3
using CSVFiles
using Dates
using NanoDates

# Write your package code here.

"""
    dataset(exchange, market) => Vector{Any}

Return OHLC candles for the given exchange and market.
"""
function dataset(exchange, market)
    [42, exchange, market]
end

function import_json!(exchange, market, timeframe; srcdir="", datadir="./data")
    dir = joinpath(srcdir, exchange, market, timeframe)
    jfs = readdir(dir; join=true)
    jfs2 = jfs[1:min(5, end)]

    # I want to fill a fixed-size bin with candles until it's full.
    # Then I want to write the bin to a file.
    # Then create a new empty fixed-size bin and repeat the process until
    #   all the candles are handled.
    boom = []
    for jf in jfs2
        candles = jf |> read |> JSON3.read
        bin = missing
        first_ts = missing
        for c in candles
            ts = c[1] ÷ 1000 |> unix2datetime
            if ismissing(bin)
                first_ts = c[1] ÷ 1000 |> unix2datetime
            end
            push!(boom, c)
        end
    end
    boom
end

# Add this to unix epoch milliseconds to get a DateTime.
_unix_epoch_ms =
    Dates.value(unix2datetime(0)) +
    abs(Dates.value(Dates.epochms2datetime(0)))

function _unixms2datetime(ms)
    Dates.epochms2datetime(_unix_epoch_ms + ms)
end

const Time0 = DateTime(1970, 1, 1)
millis2nanodate(millis::Millisecond) = Time0 + millis

end


# This is a hack I learned from the forum that allows
# LSP to recognize the current package when editing scripts.
# https://discourse.julialang.org/t/lsp-missing-reference-woes/98231/16
macro ignore(args...) end
@ignore include("../bin/fetch.jl")
@ignore include("../bin/nanotest.jl")

"""
# REPL init
using CryptoDatasets
using CryptoDatasets: import_json!
using Dates
using NanoDates
using CSVFiles
using JSON3

srcdir = "$(ENV["HOME"])/src/git.coom.tech/gg1234/ta/data"
import_json!("bitmex", "XBTUSD", "1m", srcdir=srcdir)
"""
