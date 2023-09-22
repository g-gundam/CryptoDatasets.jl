module CryptoDatasets

# Write your package code here.

"""
    dataset(exchange, market) => Vector{Any}

Return OHLC candles for the given exchange and market.
"""
function dataset(exchange, market)
    [42, exchange, market]
end

end
