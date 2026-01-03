# Bedrock Job Queue

[![Elixir CI](https://github.com/bedrock-kv/job_queue/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/bedrock-kv/job_queue/actions/workflows/elixir_ci.yaml)
[![Coverage Status](https://coveralls.io/repos/github/bedrock-kv/job_queue/badge.png?branch=main)](https://coveralls.io/github/bedrock-kv/job_queue?branch=main)

Bedrock Job Queue is a distributed job queue based on the ideas in Apple's [QuiCK](https://www.foundationdb.org/files/QuiCK.pdf) and implemented on top of [Bedrock](https://github.com/bedrock-kv/bedrock).

## Installation

The package can be installed by adding `bedrock` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bedrock_job_queue, "~> 0.1"}
  ]
end
```

## Example

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbedrock-kv%2Fjob_queue%2Frefs%2Fheads%2Fmain%2Flivebooks%2Fcoffee_shop.livemd)
