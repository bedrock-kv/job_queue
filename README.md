# Bedrock Job Queue

[![Elixir CI](https://github.com/bedrock-kv/job_queue/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/bedrock-kv/job_queue/actions/workflows/elixir_ci.yaml)
[![Coverage Status](https://coveralls.io/repos/github/bedrock-kv/job_queue/badge.png?branch=main)](https://coveralls.io/github/bedrock-kv/job_queue?branch=main)
[![Hex.pm](https://img.shields.io/hexpm/v/bedrock_job_queue.svg)](https://hex.pm/packages/bedrock_job_queue)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/bedrock_job_queue)

A durable, distributed job queue for Elixir built on [Bedrock](https://github.com/bedrock-kv/bedrock). Based on the ideas in Apple's [QuiCK paper](https://www.foundationdb.org/files/QuiCK.pdf).

## Features

- **Topic-based routing** - Route jobs to worker modules by topic
- **Priority ordering** - Lower priority numbers are processed first
- **Scheduled jobs** - Delay jobs or schedule for a specific time
- **Automatic retries** - Failed jobs retry with exponential backoff
- **Multi-tenant** - Isolate jobs by queue ID (tenant, shop, etc.)
- **Transactional** - Jobs are enqueued atomically within Bedrock transactions

## Installation

Add `bedrock_job_queue` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bedrock_job_queue, "~> 0.1"}
  ]
end
```

## Quick Start

### 1. Define your JobQueue

```elixir
defmodule MyApp.JobQueue do
  use Bedrock.JobQueue,
    otp_app: :my_app,
    repo: MyApp.Repo,
    workers: %{
      "email:send" => MyApp.Jobs.SendEmail,
      "user:welcome" => MyApp.Jobs.WelcomeUser
    }
end
```

### 2. Create job modules

```elixir
defmodule MyApp.Jobs.SendEmail do
  use Bedrock.JobQueue.Job,
    topic: "email:send",
    priority: 50,
    max_retries: 3

  @impl true
  def perform(%{to: to, subject: subject, body: body}, _meta) do
    MyApp.Mailer.send(to, subject, body)
    :ok
  end
end
```

### 3. Add to your supervision tree

```elixir
children = [
  MyApp.Cluster,
  {MyApp.JobQueue, concurrency: 10, batch_size: 5}
]
```

### 4. Enqueue jobs

```elixir
alias MyApp.JobQueue

# Immediate processing
JobQueue.enqueue("tenant_1", "email:send", %{
  to: "user@example.com",
  subject: "Hello",
  body: "Welcome!"
})

# Schedule for a specific time
JobQueue.enqueue("tenant_1", "email:send", payload,
  at: ~U[2024-01-15 10:00:00Z]
)

# Delay by duration
JobQueue.enqueue("tenant_1", "cleanup", payload,
  in: :timer.hours(1)
)

# With priority (lower = higher priority)
JobQueue.enqueue("tenant_1", "urgent", payload,
  priority: 0
)
```

## Job Return Values

Jobs can return the following values from `perform/2`:

| Return Value | Behavior |
| ------------ | -------- |
| `:ok` | Job completed successfully |
| `{:ok, result}` | Job completed with result |
| `{:error, reason}` | Job failed, will retry with backoff |
| `{:snooze, ms}` | Reschedule job after delay |
| `{:discard, reason}` | Discard job without retrying |

## Interactive Tutorial

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fraw.githubusercontent.com%2Fbedrock-kv%2Fjob_queue%2Frefs%2Fheads%2Fmain%2Flivebooks%2Fcoffee_shop.livemd)

Try the Coffee Shop tutorial in Livebook to explore job queues interactively.

## Documentation

Full documentation is available on [HexDocs](https://hexdocs.pm/bedrock_job_queue).

## License

MIT License - see [LICENSE](LICENSE) for details.
