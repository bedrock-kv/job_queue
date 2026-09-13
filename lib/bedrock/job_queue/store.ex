defmodule Bedrock.JobQueue.Store do
  @moduledoc """
  Core storage operations for the job queue.

  This module provides the transactional primitives for queue operations,
  following QuiCK paper patterns with Bedrock's ACID guarantees.

  ## Keyspace Layout

      job_queue/
        queues/{queue_id}/
          items/                         # {priority, vesting_time, id} -> Item
          priority_index/{sign, level, node} # -> earliest vesting time in priority range
          priority_index/{"root"}            # -> earliest vesting time in queue
          priority_index/{"initialized"}     # -> complete index marker
          priority_index/{"migration"}       # -> resumable legacy cursor
          identities/{item_id}           # -> canonical custom-ID Item
          identity_metadata/state         # -> current | legacy
          leases/{item_id}               # -> Lease
          dead_letter/{timestamp}/{id}   # -> Item (failed jobs after max retries)
          stats/pending                  # atomic counter
          stats/processing               # atomic counter

        queue_leases/{queue_id}          # -> QueueLease (two-tier leasing)
        pointers/                        # {vesting_time, queue_id} -> <<>>

  ## Key Encoding

  Item keys use nested tuple encoding `{priority, vesting_time, id}` which:
  - Sorts by priority first (lower = higher priority)
  - Then by vesting_time (earlier = visible first)
  - Then by id for uniqueness

  The per-queue priority index is two fixed-height min-trees, one for each
  side of the integer domain. It preserves priority-first dequeueing while
  keeping visibility checks and minimum-time reads bounded.

  Pointer keys use `{vesting_time, queue_id}` for efficient scanning of
  queues with visible items.
  """

  import Bitwise

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.Keyspace

  # The tuple encoder supports 64-bit magnitudes on either side of zero. Two
  # fixed-height subtrees retain that complete ordering without narrowing the
  # priorities accepted by existing item keys.
  @priority_bits 64
  @max_priority (1 <<< @priority_bits) - 1
  @min_priority -@max_priority
  @migration_chunk_size 8
  @priority_index_initialized_key {"initialized"}
  @priority_index_migration_key {"migration"}

  @type repo :: module()
  @type root_keyspace :: Keyspace.t()

  @doc """
  Creates keyspaces for a queue.

  Returns a map with keyspaces for item identities, items, leases, and stats.
  """
  @spec queue_keyspaces(root_keyspace(), String.t()) :: %{
          dead_letter: Keyspace.t(),
          identity_metadata: Keyspace.t(),
          identities: Keyspace.t(),
          items: Keyspace.t(),
          leases: Keyspace.t(),
          priority_index: Keyspace.t(),
          stats: Keyspace.t()
        }
  def queue_keyspaces(root, queue_id) do
    queue_ks = Keyspace.partition(root, "queues/#{queue_id}/")

    %{
      dead_letter: Keyspace.partition(queue_ks, "dead_letter/"),
      identity_metadata: Keyspace.partition(queue_ks, "identity_metadata/"),
      identities: Keyspace.partition(queue_ks, "identities/"),
      items: Keyspace.partition(queue_ks, "items/", key_encoding: TupleEncoding),
      leases: Keyspace.partition(queue_ks, "leases/"),
      priority_index: Keyspace.partition(queue_ks, "priority_index/", key_encoding: TupleEncoding),
      stats: Keyspace.partition(queue_ks, "stats/")
    }
  end

  @doc """
  Creates the pointer index keyspace.
  """
  @spec pointer_keyspace(root_keyspace()) :: Keyspace.t()
  def pointer_keyspace(root), do: Keyspace.partition(root, "pointers/", key_encoding: TupleEncoding)

  @doc """
  Creates the queue leases keyspace for two-tier leasing.
  """
  @spec queue_lease_keyspace(root_keyspace()) :: Keyspace.t()
  def queue_lease_keyspace(root), do: Keyspace.partition(root, "queue_leases/")

  @doc """
  Obtains an exclusive lease on a queue for dequeuing.

  Per QuiCK paper: Two-tier leasing prevents thundering herd. A consumer
  must first obtain a queue lease before it can dequeue items. Only one
  consumer can hold a queue lease at a time.

  Options:
  - `:now` - Fixed current time in ms (primarily useful for tests)
  - `:clock` - Zero-argument function supplying current time after the lease
    read (default: `System.system_time/1`)

  Returns:
  - `{:ok, QueueLease.t()}` - Lease obtained successfully
  - `{:error, :queue_leased}` - Queue already leased by another consumer
  """
  @spec obtain_queue_lease(
          repo(),
          root_keyspace(),
          String.t(),
          binary(),
          pos_integer(),
          keyword()
        ) ::
          {:ok, QueueLease.t()} | {:error, :queue_leased}
  def obtain_queue_lease(repo, root, queue_id, holder, duration_ms, opts \\ []) do
    ks = queue_lease_keyspace(root)
    clock = clock(opts)

    case repo.get(ks, queue_id) do
      nil ->
        # No existing lease - create new one
        now = clock.()
        lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
        repo.put(ks, queue_id, encode(lease))
        {:ok, lease}

      value ->
        existing = decode(value)
        now = clock.()

        if existing.expires_at <= now do
          # Existing lease expired - replace it
          lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
          repo.put(ks, queue_id, encode(lease))
          {:ok, lease}
        else
          # Lease still active
          {:error, :queue_leased}
        end
    end
  end

  @doc """
  Releases a queue lease.

  Should be called after finishing dequeue operations to allow other
  consumers to access the queue.
  """
  @spec release_queue_lease(repo(), root_keyspace(), QueueLease.t()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch}
  def release_queue_lease(repo, root, %QueueLease{} = lease) do
    ks = queue_lease_keyspace(root)

    case repo.get(ks, lease.queue_id) do
      nil ->
        {:error, :lease_not_found}

      value ->
        stored = decode(value)

        if stored.id == lease.id do
          repo.clear(ks, lease.queue_id)
          :ok
        else
          {:error, :lease_mismatch}
        end
    end
  end

  @doc """
  Enqueues a job item atomically.

  A supplied item ID is a queue-scoped idempotency key. The first enqueue
  persists the canonical item in a durable identity index; later calls with
  that ID resolve to the canonical item without changing queue state. The
  identity record remains after completion, so retries cannot recreate completed
  work. `enqueue/4` returns `:ok`; use `enqueue_with_item/4` when the caller
  needs the canonical item.

  Generated IDs bypass the identity index because they are already unique.

  Queues created before the identity index are detected lazily. Active legacy
  items are indexed transactionally on retry. A legacy queue with no matching
  active item rejects a custom ID with `:legacy_custom_id_unknown`, rather than
  risk recreating completed work whose ID was not historically recorded.

  Within a transaction:
  1. Writes item to queue zone with key {priority, vesting_time, id}
  2. Updates pointer index with atomic min for vesting_time
  3. Increments pending_count via atomic add
  """
  @spec enqueue(repo(), root_keyspace(), Item.t(), keyword()) ::
          :ok
          | {:error, :legacy_custom_id_unknown | :legacy_duplicate_custom_id}
  def enqueue(repo, root, %Item{} = item, opts \\ []) do
    case enqueue_with_item(repo, root, item, opts) do
      {:ok, _item} -> :ok
      error -> error
    end
  end

  @doc """
  Enqueues a job and returns the canonical item for idempotent custom IDs.

  This companion to `enqueue/4` is for callers that need the item created by
  the first enqueue rather than the retry input. `enqueue/4` preserves its
  established `:ok` return value.
  """
  @spec enqueue_with_item(repo(), root_keyspace(), Item.t(), keyword()) ::
          {:ok, Item.t()}
          | {:error, :legacy_custom_id_unknown | :legacy_duplicate_custom_id}
  def enqueue_with_item(repo, root, %Item{} = item, opts \\ []) do
    Item.validate_priority!(item.priority)
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now) || System.system_time(:millisecond)
    identity_state = identity_state(repo, keyspaces)

    if custom_id?(item, opts) do
      enqueue_custom_id(repo, keyspaces, pointers, item, now, identity_state)
    else
      write_new_item(repo, keyspaces, pointers, item, now)
    end
  end

  defp custom_id?(item, opts) do
    Keyword.get(opts, :custom_id?, Map.get(item, :custom_id?, false))
  end

  defp enqueue_custom_id(repo, keyspaces, pointers, item, now, identity_state) do
    case repo.get(keyspaces.identities, item.id) do
      nil ->
        enqueue_unindexed_custom_id(repo, keyspaces, pointers, item, now, identity_state)

      value ->
        {:ok, decode(value)}
    end
  end

  defp enqueue_unindexed_custom_id(repo, keyspaces, pointers, item, now, :current) do
    repo.put(keyspaces.identities, item.id, encode(item))
    write_new_item(repo, keyspaces, pointers, item, now)
  end

  defp enqueue_unindexed_custom_id(repo, keyspaces, _pointers, item, _now, :legacy) do
    case legacy_items_with_id(repo, keyspaces, item.id) do
      [legacy_item] ->
        repo.put(keyspaces.identities, item.id, encode(legacy_item))
        {:ok, legacy_item}

      [] ->
        {:error, :legacy_custom_id_unknown}

      _duplicate_items ->
        {:error, :legacy_duplicate_custom_id}
    end
  end

  defp identity_state(repo, keyspaces) do
    case repo.get(keyspaces.identity_metadata, "state") do
      "current" -> :current
      "legacy" -> :legacy
      nil -> initialize_identity_state(repo, keyspaces)
    end
  end

  defp initialize_identity_state(repo, keyspaces) do
    state = if legacy_queue?(repo, keyspaces), do: :legacy, else: :current
    repo.put(keyspaces.identity_metadata, "state", Atom.to_string(state))
    state
  end

  defp legacy_queue?(repo, keyspaces) do
    keyspace_has_entries?(repo, keyspaces.items) or
      keyspace_has_entries?(repo, keyspaces.dead_letter) or
      repo.get(keyspaces.stats, "pending") != nil or
      repo.get(keyspaces.stats, "processing") != nil
  end

  defp legacy_items_with_id(repo, keyspaces, item_id) do
    keyspaces.items
    |> item_keyspace_range(repo, [])
    |> Stream.map(fn {_key, value} -> decode(value) end)
    |> Enum.filter(&(&1.id == item_id))
  end

  defp keyspace_has_entries?(repo, keyspace) do
    keyspace
    |> Keyspace.prefix()
    |> Bedrock.KeyRange.from_prefix()
    |> repo.get_range(limit: 1)
    |> Enum.any?()
  end

  defp write_new_item(repo, keyspaces, pointers, item, now) do
    item_key = Item.key(item)
    repo.put(keyspaces.items, item_key, encode(item))
    refresh_priority_index_after_mutation(repo, keyspaces, item.priority)

    update_pointer(repo, pointers, item.vesting_time, item.queue_id, now)
    update_stats(repo, keyspaces, 1, 0)

    {:ok, item}
  end

  @doc """
  Peeks at visible items in a queue, ordered by priority then vesting time.

  Items are visible when:
  - vesting_time <= now
  - lease_id is nil (not currently leased)

  Options:
  - :limit - Maximum items to return (default: 10)
  - :now - Current time in ms (default: System.system_time(:millisecond))
  The priority index stores the earliest vesting time for each range of
  priorities. It makes it possible to find the next ready priority with a
  fixed number of point reads, rather than scanning future items before
  filtering them for visibility. A queue written before this index migrates in
  fixed chunks: `peek/4` returns no jobs until the final empty chunk proves
  the index covers every item, preserving global priority order throughout.
  """
  @spec peek(repo(), root_keyspace(), String.t(), keyword()) :: [Item.t()]
  def peek(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    limit = Keyword.get(opts, :limit, 10)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    case ensure_priority_index(repo, keyspaces) do
      :migrating -> []
      :empty -> []
      :indexed -> peek_ready_items(repo, keyspaces, limit, now)
    end
  end

  @doc false
  @spec migration_in_progress?(repo(), root_keyspace(), String.t()) :: boolean()
  def migration_in_progress?(repo, root, queue_id) do
    keyspaces = queue_keyspaces(root, queue_id)
    match?({:building, _cursor}, migration_cursor(repo, keyspaces.priority_index))
  end

  defp peek_ready_items(_repo, _keyspaces, 0, _now), do: []

  defp peek_ready_items(repo, keyspaces, limit, now) do
    do_peek_ready_items(repo, keyspaces, limit, now, @min_priority, [])
  end

  defp do_peek_ready_items(_repo, _keyspaces, 0, _now, _minimum_priority, items), do: Enum.reverse(items)

  defp do_peek_ready_items(repo, keyspaces, remaining, now, minimum_priority, items) do
    case next_ready_priority(repo, keyspaces, now, minimum_priority) do
      nil ->
        Enum.reverse(items)

      priority ->
        ready_items = priority_ready_items(repo, keyspaces, priority, remaining, now)
        item_count = length(ready_items)

        if priority == @max_priority do
          Enum.reverse(items, ready_items)
        else
          do_peek_ready_items(
            repo,
            keyspaces,
            remaining - item_count,
            now,
            priority + 1,
            Enum.reverse(ready_items, items)
          )
        end
    end
  end

  @doc """
  Atomically dequeues items from a queue.

  Per QuiCK paper: Combines peek + obtain_lease into a single atomic operation.
  This is more efficient than separate calls and avoids race conditions.

  ## Options

  - `:limit` - Maximum items to dequeue (default: 10)
  - `:lease_duration` - Lease duration in ms (default: 30_000)
  - `:now` - Current time in ms (default: `System.system_time(:millisecond)`)

  ## Return Value

  Returns `{:ok, [Lease.t()]}` with leases for successfully dequeued items.

  **Note:** The returned list may be smaller than `:limit` if:
  - Fewer items are visible in the queue
  - Some items were leased by other consumers between peek and obtain_lease
  - The function silently skips items that fail to lease rather than erroring

  An empty list `{:ok, []}` indicates no items were available or all visible
  items were already leased.
  """
  @spec dequeue(repo(), root_keyspace(), String.t(), binary(), keyword()) :: {:ok, [Lease.t()]}
  def dequeue(repo, root, queue_id, holder, opts \\ []) do
    limit = Keyword.get(opts, :limit, 10)
    lease_duration = Keyword.get(opts, :lease_duration, 30_000)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    # Peek for visible items
    items = peek(repo, root, queue_id, limit: limit, now: now)

    # Obtain leases on each item
    leases =
      Enum.reduce(items, [], fn item, acc ->
        case obtain_lease(repo, root, item, holder, lease_duration) do
          {:ok, lease} -> [lease | acc]
          {:error, _} -> acc
        end
      end)

    {:ok, Enum.reverse(leases)}
  end

  @doc """
  Obtains a lease on an item.

  Per QuiCK paper - leasing works by updating the vesting_time to make
  the item invisible to other consumers:

  1. Reads item, verifies it's available
  2. Creates lease record
  3. Updates item's vesting_time to lease expiry (makes it invisible)
  4. Updates pointer index with new min vesting_time

  Options:
  - `:now` - Fixed current time in ms (primarily useful for tests)
  - `:clock` - Zero-argument function supplying current time after the item
    read (default: `System.system_time/1`)
  """
  @spec obtain_lease(repo(), root_keyspace(), Item.t(), binary(), pos_integer(), keyword()) ::
          {:ok, Lease.t()} | {:error, :already_leased | :not_found}
  def obtain_lease(repo, root, %Item{} = item, holder, duration_ms, opts \\ []) do
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)
    clock = clock(opts)

    # Read current item state
    item_key = Item.key(item)

    case repo.get(keyspaces.items, item_key) do
      nil ->
        {:error, :not_found}

      value ->
        current_item = decode(value)
        now = clock.()

        if Item.leased?(current_item, now: now) do
          {:error, :already_leased}
        else
          do_obtain_lease(repo, keyspaces, pointers, current_item, holder, duration_ms, now)
        end
    end
  end

  defp do_obtain_lease(repo, keyspaces, pointers, current_item, holder, duration_ms, now) do
    lease = Lease.new(current_item, holder, duration_ms: duration_ms, now: now)
    lease_expires_at = now + duration_ms
    pending_item? = current_item.lease_id == nil

    updated_item = %{
      current_item
      | lease_id: lease.id,
        lease_expires_at: lease_expires_at,
        vesting_time: lease_expires_at
    }

    repo.clear(keyspaces.items, Item.key(current_item))

    new_item_key = Item.key(updated_item)
    repo.put(keyspaces.items, new_item_key, encode(updated_item))
    repo.put(keyspaces.leases, lease.item_id, encode(lease))
    refresh_priority_index_after_mutation(repo, keyspaces, current_item.priority)

    update_pointer(repo, pointers, lease_expires_at, current_item.queue_id, now)

    if pending_item? do
      update_stats(repo, keyspaces, -1, 1)
    end

    {:ok, lease}
  end

  @doc """
  Extends a lease's expiration time.

  Per QuiCK paper: Long-running jobs can extend their lease before expiry
  to prevent the item from becoming visible to other consumers.

  1. Validates lease exists and matches
  2. Validates the stored lease has not expired
  3. Updates item's vesting_time to new expiry
  4. Updates lease record with new expiry
  5. Updates pointer index

  ## Options

  - `:now` - Fixed current time in ms (primarily useful for tests)
  - `:clock` - Zero-argument function supplying the current time in ms; sampled
    after each lease read (default: `System.system_time/1`)

  ## Error Cases

  - `{:error, :lease_expired}` - Lease expiry has passed
  - `{:error, :lease_not_found}` - No lease record exists for this item
  - `{:error, :lease_mismatch}` - Lease ID doesn't match stored lease
  - `{:error, :item_not_found}` - Item no longer exists in queue
  """
  @spec extend_lease(repo(), root_keyspace(), Lease.t(), pos_integer(), keyword()) ::
          {:ok, Lease.t()}
          | {:error, :lease_not_found | :lease_mismatch | :lease_expired | :item_not_found}
  def extend_lease(repo, root, %Lease{} = lease, extension_ms, opts \\ []) do
    clock = clock(opts)

    if lease.expires_at <= clock.() do
      {:error, :lease_expired}
    else
      keyspaces = queue_keyspaces(root, lease.queue_id)

      case verify_active_lease(repo, keyspaces, lease, clock) do
        {:ok, stored_lease, _now} ->
          do_extend_lease(repo, root, keyspaces, stored_lease, extension_ms, clock)

        error ->
          error
      end
    end
  end

  @doc """
  Checks that a lease still belongs to this worker and has not expired.

  This check is intended immediately before invoking a job handler. It does
  not modify queue state. Call it inside a repository transaction so the read
  participates in the same conflict handling as other queue operations.
  """
  @spec lease_owned?(repo(), root_keyspace(), Lease.t(), keyword()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch | :lease_expired}
  def lease_owned?(repo, root, %Lease{} = lease, opts \\ []) do
    clock = clock(opts)
    keyspaces = queue_keyspaces(root, lease.queue_id)

    case verify_active_lease(repo, keyspaces, lease, clock) do
      {:ok, _stored_lease, _now} -> :ok
      error -> error
    end
  end

  defp do_extend_lease(repo, root, keyspaces, stored_lease, extension_ms, clock) do
    old_item_key = stored_lease.item_key

    case repo.get(keyspaces.items, old_item_key) do
      nil ->
        {:error, :item_not_found}

      item_value ->
        with {:ok, now} <- active_now(stored_lease, clock) do
          new_expires_at = now + extension_ms
          item = decode(item_value)
          updated_item = %{item | vesting_time: new_expires_at, lease_expires_at: new_expires_at}

          # Delete old item key, write with new vesting_time
          repo.clear(keyspaces.items, old_item_key)
          new_item_key = Item.key(updated_item)
          repo.put(keyspaces.items, new_item_key, encode(updated_item))
          refresh_priority_index_after_mutation(repo, keyspaces, item.priority)

          # Update lease record
          updated_lease = %{stored_lease | expires_at: new_expires_at, item_key: new_item_key}
          repo.put(keyspaces.leases, stored_lease.item_id, encode(updated_lease))

          # Update pointer index
          update_pointer(repo, pointer_keyspace(root), new_expires_at, stored_lease.queue_id, now)

          {:ok, updated_lease}
        end
    end
  end

  @doc """
  Completes a leased job, removing it from the queue.

  1. Validates lease exists and matches
  2. Validates the stored lease has not expired
  3. Deletes item from queue using stored item_key (O(1) lookup)
  4. Deletes lease record
  5. Decrements processing_count

  ## Options

  - `:now` - Fixed current time in ms (primarily useful for tests)
  - `:clock` - Zero-argument function supplying the current time in ms; sampled
    after the lease read and immediately before queue mutations (default:
    `System.system_time/1`)
  """
  @spec complete(repo(), root_keyspace(), Lease.t(), keyword()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch | :lease_expired}
  def complete(repo, root, %Lease{} = lease, opts \\ []) do
    keyspaces = queue_keyspaces(root, lease.queue_id)
    clock = clock(opts)

    with {:ok, stored_lease, _now} <- verify_active_lease(repo, keyspaces, lease, clock),
         {:ok, _now} <- active_now(stored_lease, clock) do
      item_key = stored_lease.item_key
      repo.clear(keyspaces.items, item_key)
      repo.clear(keyspaces.leases, lease.item_id)
      refresh_priority_index_after_mutation(repo, keyspaces, elem(stored_lease.item_key, 0))

      update_stats(repo, keyspaces, 0, -1)

      :ok
    end
  end

  @doc """
  Requeues a failed job with exponential backoff.

  1. Increments error_count
  2. If exhausted, moves to dead letter queue
  3. Otherwise, sets new vesting_time with backoff
  4. Clears lease

  ## Options

  - `:backoff_fn` - Function `(attempt) -> delay_ms` for retry delay
  - `:base_delay` - Fixed base delay in ms (used by snooze, overrides backoff_fn)
  - `:max_delay` - Maximum delay in ms (default: 60_000)
  - `:now` - Fixed current time in ms (primarily useful for tests)
  - `:clock` - Zero-argument function supplying the current time in ms; sampled
    after required reads and immediately before queue mutations (default:
    `System.system_time/1`)

  ## Error Cases

  - `{:error, :lease_not_found}` - No lease record exists for this item
  - `{:error, :lease_mismatch}` - Lease ID doesn't match stored lease
  - `{:error, :lease_expired}` - Lease expiry has passed
  - `{:error, :item_not_found}` - Item no longer exists in queue
  """
  @spec requeue(repo(), root_keyspace(), Lease.t(), keyword()) ::
          {:ok, :requeued | :dead_lettered}
          | {:error, :lease_not_found | :lease_mismatch | :lease_expired | :item_not_found}
  def requeue(repo, root, %Lease{} = lease, opts) do
    keyspaces = queue_keyspaces(root, lease.queue_id)
    pointers = pointer_keyspace(root)
    clock = clock(opts)

    with {:ok, stored_lease, _now} <- verify_active_lease(repo, keyspaces, lease, clock),
         item_key = stored_lease.item_key,
         {:ok, item} <- fetch_item(repo, keyspaces, item_key) do
      do_requeue(repo, keyspaces, pointers, {lease, stored_lease}, item, item_key, opts, clock)
    end
  end

  defp fetch_item(repo, keyspaces, item_key) do
    case repo.get(keyspaces.items, item_key) do
      nil -> {:error, :item_not_found}
      value -> {:ok, decode(value)}
    end
  end

  defp do_requeue(repo, keyspaces, pointers, {lease, stored_lease}, item, item_key, opts, clock) do
    plan = requeue_plan(item, opts)

    with {:ok, now} <- active_now(stored_lease, clock) do
      write_requeue(repo, keyspaces, pointers, lease, item, item_key, plan, now)
    end
  end

  defp requeue_plan(item, opts) do
    new_error_count = item.error_count + 1

    if new_error_count >= item.max_retries do
      {:dead_letter, new_error_count}
    else
      {:requeue, new_error_count, calculate_backoff_delay(opts, new_error_count)}
    end
  end

  defp write_requeue(repo, keyspaces, _pointers, lease, item, item_key, {:dead_letter, _error_count}, now) do
    move_to_dead_letter(repo, keyspaces, item_key, item, now)
    repo.clear(keyspaces.leases, lease.item_id)
    {:ok, :dead_lettered}
  end

  defp write_requeue(repo, keyspaces, pointers, lease, item, item_key, {:requeue, error_count, delay}, now) do
    new_vesting_time = now + delay

    updated_item = %{
      item
      | error_count: error_count,
        vesting_time: new_vesting_time,
        lease_id: nil,
        lease_expires_at: nil
    }

    repo.clear(keyspaces.items, item_key)
    new_item_key = Item.key(updated_item)
    repo.put(keyspaces.items, new_item_key, encode(updated_item))
    refresh_priority_index_after_mutation(repo, keyspaces, item.priority)

    update_pointer(repo, pointers, new_vesting_time, lease.queue_id, now)
    repo.clear(keyspaces.leases, lease.item_id)
    update_stats(repo, keyspaces, 1, -1)

    {:ok, :requeued}
  end

  # Calculate backoff delay based on options.
  # If :backoff_fn is provided, use it (standard retry behavior).
  # If :base_delay is provided, use fixed delay with exponential multiplier (snooze behavior).
  defp calculate_backoff_delay(opts, error_count) do
    cond do
      # Explicit base_delay overrides backoff_fn (used by snooze)
      base_delay = Keyword.get(opts, :base_delay) ->
        max_delay = Keyword.get(opts, :max_delay, 60_000)
        (base_delay * :math.pow(2, error_count - 1)) |> trunc() |> min(max_delay)

      backoff_fn = Keyword.get(opts, :backoff_fn) ->
        backoff_fn.(error_count)

      true ->
        (1000 * :math.pow(2, error_count - 1)) |> trunc() |> min(60_000)
    end
  end

  @doc """
  Gets queue statistics.
  """
  @spec stats(repo(), root_keyspace(), String.t()) :: %{
          pending_count: non_neg_integer(),
          processing_count: non_neg_integer()
        }
  def stats(repo, root, queue_id) do
    keyspaces = queue_keyspaces(root, queue_id)

    pending = decode_counter(repo.get(keyspaces.stats, "pending"))
    processing = decode_counter(repo.get(keyspaces.stats, "processing"))

    %{pending_count: pending, processing_count: processing}
  end

  @doc """
  Gets the minimum vesting_time from items in a queue.

  Per QuiCK Algorithm 2: After dequeuing, read the minimum vesting_time to
  determine when to next scan this queue. Returns nil if queue is empty.

  The priority index root stores this value exactly, so indexed queues need one
  point read rather than a bounded approximation over priority-ordered item
  rows. While an upgraded queue is migrating in fixed chunks, this returns `0`
  as an immediate-rescan sentinel; it is not an exact minimum until migration
  completes.

  ## Options

  - `:advance_migration?` - Whether this call may process one migration chunk
    (default: `true`). The consumer passes `false` after `peek/4` so one
    manager transaction cannot consume multiple chunks.
  """
  @spec min_vesting_time(repo(), root_keyspace(), String.t(), keyword()) ::
          non_neg_integer() | nil
  def min_vesting_time(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    advance_migration? = Keyword.get(opts, :advance_migration?, true)

    case minimum_priority_index_status(repo, keyspaces, advance_migration?) do
      :migrating -> 0
      :empty -> nil
      :indexed -> priority_index_minimum(repo, keyspaces)
    end
  end

  @doc """
  Updates a queue's pointer with a new vesting_time.

  Per QuiCK Algorithm 2: After processing, update the pointer to the minimum
  vesting_time of remaining items. This prevents rescanning queues that only
  have future-scheduled items.

  When the new vesting_time is in the future, this also cleans up any stale
  pointers in the past to prevent the scanner from repeatedly finding them.
  """
  @spec update_queue_pointer(repo(), root_keyspace(), String.t(), non_neg_integer(), keyword()) ::
          :ok
  def update_queue_pointer(repo, root, queue_id, vesting_time, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now) || System.system_time(:millisecond)

    # If new vesting_time is in the future, clean up any stale pointers in the past
    # This prevents the scanner from repeatedly finding stale pointers that point
    # to queues where all visible items have been processed
    if vesting_time > now do
      cleanup_past_pointers(repo, pointers, queue_id, now)
    end

    update_pointer(repo, pointers, vesting_time, queue_id, now)
    :ok
  end

  # Cleans up pointers for a queue_id that are in the past (vesting_time <= now).
  # This is called when updating to a future vesting_time to remove stale pointers.
  defp cleanup_past_pointers(repo, pointers, queue_id, now) do
    {start_key, end_key} = pointer_visible_range(now)
    prefix = Keyspace.prefix(pointers)

    {prefix <> start_key, prefix <> end_key}
    |> repo.get_range(limit: 100)
    |> Enum.each(fn {key, _value} ->
      suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
      {vesting_time, pointer_queue_id} = unpack_pointer_key(suffix)

      # Only delete pointers for our queue_id
      if pointer_queue_id == queue_id do
        repo.clear(pointers, {vesting_time, pointer_queue_id})
      end
    end)
  end

  @doc """
  Scans the pointer index for queues with visible items.

  Returns queue_ids that have items with vesting_time <= now.
  """
  @spec scan_visible_queues(repo(), root_keyspace(), keyword()) :: [String.t()]
  def scan_visible_queues(repo, root, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    limit = Keyword.get(opts, :limit, 100)

    # Range from 0 to now+1 (exclusive)
    {start_key, end_key} = pointer_visible_range(now)
    prefix = Keyspace.prefix(pointers)

    {prefix <> start_key, prefix <> end_key}
    |> repo.get_range(limit: limit)
    |> Enum.map(fn {key, _value} ->
      suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
      {_vesting_time, queue_id} = unpack_pointer_key(suffix)
      queue_id
    end)
    |> Enum.uniq()
  end

  @doc """
  Garbage collects stale pointers.

  Per QuiCK paper: Pointers become stale when their vesting_time has passed
  and the queue has no visible items. This function scans for such pointers
  and deletes them.

  Options:
  - :limit - Maximum pointers to scan (default: 100)
  - :grace_period - Additional time in ms after vesting before GC (default: 60_000)
  - :now - Current time in ms (default: System.system_time(:millisecond))

  Returns the count of deleted pointers.
  """
  @spec gc_stale_pointers(repo(), root_keyspace(), keyword()) :: non_neg_integer()
  def gc_stale_pointers(repo, root, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    grace_period = Keyword.get(opts, :grace_period, 60_000)
    limit = Keyword.get(opts, :limit, 100)

    # Scan pointers that are past their vesting_time + grace period
    cutoff = now - grace_period
    {start_key, end_key} = pointer_visible_range(cutoff)
    prefix = Keyspace.prefix(pointers)

    stale_pointers =
      {prefix <> start_key, prefix <> end_key}
      |> repo.get_range(limit: limit)
      |> Enum.map(fn {key, value} ->
        suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
        {vesting_time, queue_id} = unpack_pointer_key(suffix)
        last_active_time = decode_timestamp(value)
        {key, vesting_time, queue_id, last_active_time}
      end)

    # Per QuiCK paper: Delete pointer only if last_active_time + grace_period < now
    # AND queue is actually empty
    Enum.reduce(stale_pointers, 0, fn pointer_info, count ->
      count + maybe_delete_pointer(repo, root, pointer_info, now, grace_period)
    end)
  end

  defp maybe_delete_pointer(repo, root, {key, _vesting_time, queue_id, last_active_time}, now, grace_period) do
    inactive? = now - last_active_time >= grace_period
    empty? = inactive? && queue_empty?(repo, root, queue_id)

    if inactive? && empty? do
      repo.clear(key)
      1
    else
      0
    end
  end

  defp queue_empty?(repo, root, queue_id) do
    keyspaces = queue_keyspaces(root, queue_id)
    ensure_priority_index(repo, keyspaces) == :empty
  end

  # Private helpers

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)

  # Verifies a lease exists and matches the provided lease ID
  defp verify_lease(repo, keyspaces, %Lease{} = lease) do
    case repo.get(keyspaces.leases, lease.item_id) do
      nil ->
        {:error, :lease_not_found}

      value ->
        stored = decode(value)
        if stored.id == lease.id, do: {:ok, stored}, else: {:error, :lease_mismatch}
    end
  end

  # Finalization must observe the same active-ownership condition as execution:
  # an ID match alone is insufficient once another consumer may claim the item.
  # Sample time only after reading the stored lease so a blocked read cannot
  # authorize work past the stored expiration deadline.
  defp verify_active_lease(repo, keyspaces, %Lease{} = lease, clock) do
    with {:ok, stored_lease} <- verify_lease(repo, keyspaces, lease),
         {:ok, now} <- active_now(stored_lease, clock) do
      {:ok, stored_lease, now}
    end
  end

  defp active_now(%Lease{} = lease, clock) do
    now = clock.()
    if lease.expires_at > now, do: {:ok, now}, else: {:error, :lease_expired}
  end

  defp clock(opts) do
    case Keyword.fetch(opts, :clock) do
      {:ok, clock} when is_function(clock, 0) -> clock
      :error -> fixed_clock(Keyword.get(opts, :now))
    end
  end

  defp fixed_clock(nil), do: fn -> System.system_time(:millisecond) end
  defp fixed_clock(now), do: fn -> now end

  # Updates the pointer index with a new vesting time.
  # Per QuiCK paper: stores last_active_time (when items were last seen) for smarter GC.
  # Uses repo.max to track the most recent activity time.
  defp update_pointer(repo, pointers, vesting_time, queue_id, now) do
    pointer_key = Keyspace.pack(pointers, {vesting_time, queue_id})
    repo.max(pointer_key, encode_timestamp(now))
  end

  defp encode_timestamp(time), do: <<time::64-little>>

  defp decode_timestamp(nil), do: 0
  defp decode_timestamp(<<time::64-little>>), do: time

  defp item_keyspace_range(keyspace, repo, opts) do
    prefix = Keyspace.prefix(keyspace)

    prefix
    |> Bedrock.KeyRange.from_prefix()
    |> repo.get_range(opts)
    |> Stream.filter(fn {key, _value} -> item_storage_key?(key, prefix) end)
  end

  defp item_storage_key?(key, prefix) do
    prefix_len = byte_size(prefix)

    case key do
      <<^prefix::binary-size(prefix_len), suffix::binary>> -> item_key_suffix?(suffix)
      _ -> false
    end
  end

  defp item_key_suffix?(suffix) do
    case TupleEncoding.unpack(suffix) do
      {priority, vesting_time, id}
      when is_integer(priority) and is_integer(vesting_time) and is_binary(id) ->
        true

      _ ->
        false
    end
  rescue
    ArgumentError -> false
  end

  # Atomically updates pending and processing stats
  defp update_stats(repo, keyspaces, pending_delta, processing_delta) do
    if pending_delta != 0 do
      pending_key = Keyspace.pack(keyspaces.stats, "pending")
      repo.add(pending_key, <<pending_delta::64-signed-little>>)
    end

    if processing_delta != 0 do
      processing_key = Keyspace.pack(keyspaces.stats, "processing")
      repo.add(processing_key, <<processing_delta::64-signed-little>>)
    end
  end

  defp decode_counter(nil), do: 0
  defp decode_counter(<<>>), do: 0

  defp decode_counter(<<value::64-signed-little>>), do: max(0, value)

  defp decode_counter(binary) when is_binary(binary) and byte_size(binary) <= 8 do
    # Handle variable-length little-endian (up to 64 bits)
    size = byte_size(binary) * 8
    <<value::size(size)-signed-little>> = binary
    max(0, value)
  end

  # If binary is larger than 8 bytes, something is wrong - return 0
  defp decode_counter(_), do: 0

  defp move_to_dead_letter(repo, keyspaces, item_key, item, now) do
    # Write to dead letter with failed_at timestamp
    dl_key = "#{now}/#{item.id}"
    repo.put(keyspaces.dead_letter, dl_key, encode(item))

    # Delete from main queue and update stats
    repo.clear(keyspaces.items, item_key)
    refresh_priority_index_after_mutation(repo, keyspaces, item.priority)
    update_stats(repo, keyspaces, 0, -1)
  end

  # Priority index

  # The index is two fixed-height binary min-trees: negative priorities come
  # first, then non-negative priorities. Their root values are folded into one
  # queue minimum. This preserves the full priority range accepted by item keys
  # while keeping every current-format lookup bounded.

  defp priority_index_present?(repo, keyspaces), do: not is_nil(priority_index_minimum(repo, keyspaces))

  # A root is exact only after the initialized marker has been written. Older
  # queues are migrated in resumable chunks; their partial tree is deliberately
  # never used to dispatch work or report a precise minimum.
  defp ensure_priority_index(repo, keyspaces) do
    index = keyspaces.priority_index

    case migration_cursor(repo, index) do
      {:building, cursor} ->
        advance_priority_migration(repo, keyspaces, cursor)

      nil ->
        cond do
          priority_index_initialized?(repo, index) ->
            priority_index_status(repo, keyspaces)

          priority_index_present?(repo, keyspaces) ->
            # Indexes written by the first indexed release have a complete
            # root but no lifecycle marker. Adopt them without rebuilding.
            put_priority_index_initialized(repo, index)
            :indexed

          true ->
            start_priority_migration(repo, keyspaces)
        end
    end
  end

  # Manager transactions call peek/4 followed by min_vesting_time/4. A
  # migration marker means peek already consumed this transaction's one chunk,
  # so minimum reads the progress sentinel rather than consuming another.
  # A direct minimum query still starts and advances a migration when needed.
  defp minimum_priority_index_status(repo, keyspaces, true) do
    case migration_cursor(repo, keyspaces.priority_index) do
      {:building, cursor} -> advance_priority_migration(repo, keyspaces, cursor)
      nil -> ensure_priority_index(repo, keyspaces)
    end
  end

  defp minimum_priority_index_status(repo, keyspaces, false) do
    case migration_cursor(repo, keyspaces.priority_index) do
      {:building, _cursor} ->
        :migrating

      nil ->
        index = keyspaces.priority_index

        cond do
          priority_index_initialized?(repo, index) ->
            priority_index_status(repo, keyspaces)

          priority_index_present?(repo, keyspaces) ->
            # This is the complete root written by the first indexed release;
            # adopting it does not scan or advance a migration chunk.
            put_priority_index_initialized(repo, index)
            :indexed

          true ->
            # `advance_migration?: false` is used only after peek/4 by the
            # manager. Do not let it start a migration outside that bounded
            # peek step if a caller uses it independently.
            :migrating
        end
    end
  end

  defp start_priority_migration(repo, keyspaces) do
    index = keyspaces.priority_index
    repo.clear_range(index)
    put_migration_cursor(repo, index, nil)
    advance_priority_migration(repo, keyspaces, nil)
  end

  defp advance_priority_migration(repo, keyspaces, cursor) do
    {start_key, end_key} = migration_item_range(keyspaces.items, cursor)

    rows =
      {start_key, end_key}
      |> repo.get_range(limit: @migration_chunk_size)
      |> Enum.to_list()

    Enum.each(rows, &merge_migrated_item(repo, keyspaces, &1))

    case List.last(rows) do
      nil ->
        index = keyspaces.priority_index
        repo.clear(index, @priority_index_migration_key)
        put_priority_index_initialized(repo, index)
        priority_index_status(repo, keyspaces)

      {last_key, _value} ->
        put_migration_cursor(repo, keyspaces.priority_index, last_key)
        :migrating
    end
  end

  defp migration_item_range(item_keyspace, nil) do
    item_keyspace
    |> Keyspace.prefix()
    |> Bedrock.KeyRange.from_prefix()
  end

  defp migration_item_range(item_keyspace, cursor) do
    {_start_key, end_key} =
      item_keyspace
      |> Keyspace.prefix()
      |> Bedrock.KeyRange.from_prefix()

    {Bedrock.Key.key_after(cursor), end_key}
  end

  defp merge_migrated_item(repo, keyspaces, {key, value}) do
    if item_storage_key?(key, Keyspace.prefix(keyspaces.items)) do
      merge_priority_index(repo, keyspaces, decode(value))
    end
  end

  defp priority_index_status(repo, keyspaces) do
    if priority_index_present?(repo, keyspaces), do: :indexed, else: :empty
  end

  defp priority_index_initialized?(repo, index), do: repo.get(index, @priority_index_initialized_key) == "ready"

  defp put_priority_index_initialized(repo, index), do: repo.put(index, @priority_index_initialized_key, "ready")

  defp migration_cursor(repo, index) do
    case repo.get(index, @priority_index_migration_key) do
      nil -> nil
      value -> decode(value)
    end
  end

  defp put_migration_cursor(repo, index, cursor),
    do: repo.put(index, @priority_index_migration_key, encode({:building, cursor}))

  defp refresh_priority_index_after_mutation(repo, keyspaces, priority) do
    case ensure_priority_index(repo, keyspaces) do
      :empty -> :ok
      _status -> refresh_priority_index(repo, keyspaces, priority)
    end
  end

  defp refresh_priority_index(repo, keyspaces, priority) do
    minimum = priority_minimum(repo, keyspaces.items, priority)
    {sign, leaf} = priority_location(priority)
    index = keyspaces.priority_index

    put_priority_node(repo, index, {sign, @priority_bits, leaf}, minimum)
    refresh_priority_ancestors(repo, index, sign, @priority_bits - 1, div(leaf, 2))
    refresh_priority_root(repo, index)
  end

  defp merge_priority_index(repo, keyspaces, item) do
    {sign, leaf} = priority_location(item.priority)
    index = keyspaces.priority_index
    key = {sign, @priority_bits, leaf}

    case priority_node(repo, index, key) do
      nil ->
        put_priority_node(repo, index, key, item.vesting_time)
        refresh_priority_ancestors(repo, index, sign, @priority_bits - 1, div(leaf, 2))
        refresh_priority_root(repo, index)

      current_minimum when item.vesting_time < current_minimum ->
        put_priority_node(repo, index, key, item.vesting_time)
        refresh_priority_ancestors(repo, index, sign, @priority_bits - 1, div(leaf, 2))
        refresh_priority_root(repo, index)

      _current_minimum ->
        :ok
    end
  end

  defp priority_location(priority)
       when is_integer(priority) and priority >= @min_priority and priority <= @max_priority do
    if priority < 0, do: {0, priority - @min_priority}, else: {1, priority}
  end

  defp priority_location(priority) do
    raise ArgumentError,
          "priority must be an integer between #{@min_priority} and #{@max_priority}, got: #{inspect(priority)}"
  end

  defp priority_minimum(repo, item_keyspace, priority) do
    item_keyspace
    |> priority_item_range(priority, repo, limit: 1)
    |> Stream.map(fn {_key, value} -> decode(value) end)
    |> Enum.at(0)
    |> case do
      nil -> nil
      item -> item.vesting_time
    end
  end

  defp priority_item_range(item_keyspace, priority, repo, opts) do
    start_key = Keyspace.pack(item_keyspace, {priority, 0, <<>>})

    end_key =
      if priority == @max_priority do
        item_keyspace |> Keyspace.prefix() |> Bedrock.KeyRange.from_prefix() |> elem(1)
      else
        Keyspace.pack(item_keyspace, {priority + 1, 0, <<>>})
      end

    repo.get_range({start_key, end_key}, opts)
  end

  defp refresh_priority_ancestors(repo, index, sign, level, node) do
    left = priority_node(repo, index, {sign, level + 1, node * 2})
    right = priority_node(repo, index, {sign, level + 1, node * 2 + 1})
    put_priority_node(repo, index, {sign, level, node}, minimum(left, right))

    if level > 0 do
      refresh_priority_ancestors(repo, index, sign, level - 1, div(node, 2))
    end
  end

  defp refresh_priority_root(repo, index) do
    negative_minimum = priority_node(repo, index, {0, 0, 0})
    non_negative_minimum = priority_node(repo, index, {1, 0, 0})
    put_priority_node(repo, index, {"root"}, minimum(negative_minimum, non_negative_minimum))
  end

  defp priority_index_minimum(repo, keyspaces), do: priority_node(repo, keyspaces.priority_index, {"root"})

  defp priority_node(repo, index, key) do
    case repo.get(index, key) do
      nil -> nil
      value -> decode_timestamp(value)
    end
  end

  defp put_priority_node(repo, index, key, nil), do: repo.clear(index, key)
  defp put_priority_node(repo, index, key, time), do: repo.put(index, key, encode_timestamp(time))

  defp next_ready_priority(repo, keyspaces, now, minimum_priority) do
    index = keyspaces.priority_index

    case next_ready_negative_priority(repo, index, now, minimum_priority) do
      nil -> next_ready_non_negative_priority(repo, index, now, minimum_priority)
      priority -> priority
    end
  end

  defp next_ready_negative_priority(_repo, _index, _now, minimum_priority) when minimum_priority > -1, do: nil

  defp next_ready_negative_priority(repo, index, now, minimum_priority) do
    minimum_leaf = max(minimum_priority, @min_priority) - @min_priority

    case find_ready_leaf(repo, index, 0, now, minimum_leaf, 0, 0, {0, @max_priority}) do
      nil -> nil
      leaf -> leaf + @min_priority
    end
  end

  defp next_ready_non_negative_priority(_repo, _index, _now, minimum_priority) when minimum_priority > @max_priority,
    do: nil

  defp next_ready_non_negative_priority(repo, index, now, minimum_priority) do
    minimum_leaf = max(minimum_priority, 0)
    find_ready_leaf(repo, index, 1, now, minimum_leaf, 0, 0, {0, @max_priority})
  end

  defp find_ready_leaf(_repo, _index, _sign, _now, minimum_leaf, _level, _node, {_low, high}) when high < minimum_leaf,
    do: nil

  defp find_ready_leaf(repo, index, sign, now, minimum_leaf, level, node, {low, high}) do
    case priority_node(repo, index, {sign, level, node}) do
      nil ->
        nil

      vesting_time when vesting_time > now ->
        nil

      _vesting_time when level == @priority_bits ->
        low

      _vesting_time ->
        midpoint = low + div(high - low, 2)

        case find_ready_leaf(repo, index, sign, now, minimum_leaf, level + 1, node * 2, {low, midpoint}) do
          nil ->
            find_ready_leaf(
              repo,
              index,
              sign,
              now,
              minimum_leaf,
              level + 1,
              node * 2 + 1,
              {midpoint + 1, high}
            )

          leaf ->
            leaf
        end
    end
  end

  defp priority_ready_items(repo, keyspaces, priority, limit, now) do
    keyspaces.items
    |> priority_item_range(priority, repo, limit: limit)
    |> Stream.map(fn {_key, value} -> decode(value) end)
    |> Stream.filter(&Item.visible?(&1, now))
    |> Enum.to_list()
  end

  defp minimum(nil, value), do: value
  defp minimum(value, nil), do: value
  defp minimum(left, right), do: min(left, right)

  # Pointer key helpers (replacing PointerKey module)

  defp pointer_visible_range(now) do
    start_key = TupleEncoding.pack({0, <<>>})
    end_key = TupleEncoding.pack({now + 1, <<>>})
    {start_key, end_key}
  end

  defp unpack_pointer_key(suffix) do
    TupleEncoding.unpack(suffix)
  end
end
