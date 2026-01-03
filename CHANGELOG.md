# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
