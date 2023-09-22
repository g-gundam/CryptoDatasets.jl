using CryptoDatasets
using Documenter

DocMeta.setdocmeta!(CryptoDatasets, :DocTestSetup, :(using CryptoDatasets); recursive=true)

makedocs(;
    modules=[CryptoDatasets],
    authors="g-gundam <gg@nowhere> and contributors",
    repo="https://github.com/g-gundam/CryptoDatasets.jl/blob/{commit}{path}#{line}",
    sitename="CryptoDatasets.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://g-gundam.github.io/CryptoDatasets.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/g-gundam/CryptoDatasets.jl",
    devbranch="main",
)
