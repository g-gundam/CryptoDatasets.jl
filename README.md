# CryptoDatasets

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://g-gundam.github.io/CryptoDatasets.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://g-gundam.github.io/CryptoDatasets.jl/dev/)
[![Build Status](https://github.com/g-gundam/CryptoDatasets.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/g-gundam/CryptoDatasets.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/g-gundam/CryptoDatasets.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/g-gundam/CryptoDatasets.jl)

## Goals

- Allow the download of archived market data from select crypto exchanges.
  + This should reduce the load from official API servers for this data.
  + Sometimes, this data is also hard to get from official APIs.
- This data will be in the form of 1m candles stored in CSV files that contain 1 day of 1m candles.
  + Higher timeframe data can be derived from the 1m data, so I'm not going to provide it as raw data.
- This raw data should be generally useful to users of other languages too.
  + I'd like this to be a language neutral project.
  + I'd also like this to be a very gentle introduction to what Julia has to offer.
