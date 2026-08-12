# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-08-12

### Added

- Queue action hooks via a new `:on_action` option on `use Bedrock.JobQueue`
  (also accepted by the Consumer and Supervisor). The hook is a
  `{module, function}` or `{module, function, extra_args}` tuple invoked
  **inside the same transaction** as the job's completion/requeue action,
  receiving the repo, root keyspace, lease, action, handler result, and queue
  result. Returning `{:error, reason}` aborts the transaction. This lets you
  atomically record job outcomes alongside queue state — for example,
  maintaining an audit trail or intent ledger:

  ```elixir
  defmodule MyApp.JobQueue do
    use Bedrock.JobQueue,
      otp_app: :my_app,
      repo: MyApp.Repo,
      workers: %{"emails" => MyApp.EmailJob},
      on_action: {MyApp.Ledger, :record_job_action, []}
  end
  ```

### Fixed

- Queue item scans (peek, next-vesting-time, and empty checks) now scan raw
  key ranges by prefix. Item keys are tuple-encoded as
  `{priority, vesting_time, id}`, and keyspace range decoding raised on the
  nested tuple suffixes these scans return.
- Dead-lettered jobs are now stored in a sibling keyspace, and item scans
  filter by valid item key shape, so dead letters and legacy rows can no
  longer crash or block queue scans.
- Completing or requeuing a job now always uses the item key from the stored
  (verified) lease rather than trusting the caller-supplied lease, preventing
  a stale lease from clearing the wrong item.
- The lease extender now handles lease-extension transaction results
  correctly, so successful extensions are no longer logged (and treated) as
  failures.

## [0.1.0] - 2025-01-03

### Added

- Initial release of Bedrock Job Queue
- `Bedrock.JobQueue` - Main module with `use` macro for defining job queues
- `Bedrock.JobQueue.Job` - Behaviour for defining job workers with `perform/2` callback
- Topic-based routing to worker modules via workers map
- Priority ordering (lower numbers = higher priority)
- Scheduled jobs with `:at` (DateTime) and `:in` (delay in ms) options
- Automatic retries with exponential backoff
- Multi-tenant support via queue IDs
- Transactional enqueueing within Bedrock transactions
- Consumer architecture with Scanner, Manager, and Worker processes
- Configurable concurrency and batch size
- Queue statistics via `stats/2`
- Job return values: `:ok`, `{:ok, result}`, `{:error, reason}`, `{:snooze, ms}`, `{:discard, reason}`
- Coffee Shop interactive Livebook tutorial
