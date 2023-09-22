using CryptoDatasets
using Test

@testset "CryptoDatasets.jl" begin
    # Write your tests here.
    @test Crypto.dataset("bybit", "BTCUSD") == [42, "bybit", "BTCUSD"]
end
