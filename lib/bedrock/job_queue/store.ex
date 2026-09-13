defmodule Bedrock.JobQueue.Store do
  @moduledoc """
  Core storage operations for the job queue.

  This module provides the transactional primitives for queue operations,
  following QuiCK paper patterns with Bedrock's ACID guarantees.

  ## Keyspace Layout

      job_queue/
        queues/{queue_id}/
          items/                         # {priority, vesting_time, id} -> Item
          priority_index/                    # legacy v1 tree (inert after upgrade)
          priority_index/v2/{sign, level, node} # -> earliest vesting time in priority range
          priority_index/v2/{"vesting", sign, priority, level, node}
                                           # -> earliest nonempty time in a priority
          priority_index/v2/{"member", sign, priority, vesting_time, id}
                                           # -> one row per indexed item
          priority_index/v2/{"root"}            # -> earliest vesting time in queue
          priority_index/v2/{"initialized"}     # -> complete v2 index marker
          priority_index/v2/{"migration"}       # -> fenced, resumable raw-item cursor
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

  The per-queue priority index has two fixed-height min-trees, one for each
  side of the integer domain, plus a fixed-height vesting-time multiset tree
  for every active priority. It preserves priority-first dequeueing while
  keeping visibility checks and minimum-time reads bounded even after a
  transaction moves or removes the first raw item row.

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
  @vesting_bits 64
  @max_priority (1 <<< @priority_bits) - 1
  @min_priority -@max_priority
  @migration_chunk_size 8
  @priority_index_initialized_key {"initialized"}
  @priority_index_migration_key {"migration"}

  @type repo :: module()
  @type root_keyspace :: Keyspace.t()
  @type priority_index_status :: :writer_fence_required | :migrating | :ready | :empty
  @type timestamp_error :: :vesting_time_out_of_range

  @doc """
  Creates keyspaces for a queue.

  Returns a map with keyspaces for item identities, items, leases, stats, and
  the current v2 plus inert legacy priority indexes.
  """
  @spec queue_keyspaces(root_keyspace(), String.t()) :: %{
          dead_letter: Keyspace.t(),
          identity_metadata: Keyspace.t(),
          identities: Keyspace.t(),
          items: Keyspace.t(),
          legacy_priority_index: Keyspace.t(),
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
      legacy_priority_index: Keyspace.partition(queue_ks, "priority_index/", key_encoding: TupleEncoding),
      leases: Keyspace.partition(queue_ks, "leases/"),
      priority_index: Keyspace.partition(queue_ks, "priority_index/v2/", key_encoding: TupleEncoding),
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
  Returns the scheduling-index state for a queue.

  `:writer_fence_required` means the queue contains pre-v2 work or an
  untrusted v2 marker. It is deliberately held
  until an administrator calls `migrate_priority_index/4` after fencing all
  old writers. A marker written by new code cannot fence an older writer that
  does not read it. Only `:ready` and `:empty` queues permit direct Store
  mutations, leases, or pointer updates; every held status returns
  `{:error, :priority_index_migration_required}` from those operations.
  """
  @spec priority_index_status(repo(), root_keyspace(), String.t()) :: priority_index_status()
  def priority_index_status(repo, root, queue_id) do
    root
    |> queue_keyspaces(queue_id)
    |> priority_index_state(repo)
  end

  @doc """
  Advances one bounded chunk of an explicitly writer-fenced legacy migration.

  The operator must stop every pre-index producer and consumer for this queue
  and ensure none can resume. Pass `writer_fence: :offline` on every call to
  acknowledge that operational precondition: an old writer cannot observe a
  new marker, so no in-band key can enforce the fence for it.

  The queue must remain static for every call, until this function returns
  `:ready` or `:empty`. While the status is `:migrating`, normal queue
  operations are held with `{:error, :priority_index_migration_required}` and
  the Manager does not dispatch it. Each call reads and indexes at most
  #{@migration_chunk_size} raw item rows into a fresh v2 keyspace; v1 is never
  read or cleared. A marker-linked partial v2 tree is never a dispatch source.
  A short final range proves the static queue has been covered, so that same
  call atomically activates the complete v2 index.
  """
  @spec migrate_priority_index(repo(), root_keyspace(), String.t(), keyword()) ::
          :more | :ready | :empty | {:error, :writer_fence_required}
  def migrate_priority_index(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    migrate_priority_index_state(repo, keyspaces, migration_state(repo, keyspaces.priority_index), opts)
  end

  defp migrate_priority_index_state(repo, keyspaces, migration_state, opts) do
    case priority_index_state(keyspaces, repo, migration_state) do
      :writer_fence_required ->
        if Keyword.get(opts, :writer_fence) == :offline and is_nil(migration_state) do
          advance_priority_migration(repo, keyspaces, nil)
        else
          {:error, :writer_fence_required}
        end

      :migrating ->
        if Keyword.get(opts, :writer_fence) == :offline do
          advance_offline_priority_migration(repo, keyspaces, migration_state)
        else
          {:error, :writer_fence_required}
        end

      :ready ->
        :ready

      :empty ->
        :empty
    end
  end

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
          {:ok, QueueLease.t()}
          | {:error, :queue_leased | :priority_index_migration_required | timestamp_error()}
  def obtain_queue_lease(repo, root, queue_id, holder, duration_ms, opts \\ []) do
    ks = queue_lease_keyspace(root)
    clock = clock(opts)

    with :ok <- require_queue_operation(repo, queue_keyspaces(root, queue_id)) do
      case repo.get(ks, queue_id) do
        nil ->
          # No existing lease - create new one
          now = clock.()

          with {:ok, _expires_at} <- future_vesting_time(now, duration_ms) do
            lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
            repo.put(ks, queue_id, encode(lease))
            {:ok, lease}
          end

        value ->
          existing = decode(value)
          now = clock.()

          if existing.expires_at <= now do
            # Existing lease expired - replace it
            with {:ok, _expires_at} <- future_vesting_time(now, duration_ms) do
              lease = QueueLease.new(queue_id, holder, duration_ms: duration_ms, now: now)
              repo.put(ks, queue_id, encode(lease))
              {:ok, lease}
            end
          else
            # Lease still active
            {:error, :queue_leased}
          end
      end
    end
  end

  @doc """
  Releases a queue lease.

  Should be called after finishing dequeue operations to allow other
  consumers to access the queue.
  """
  @spec release_queue_lease(repo(), root_keyspace(), QueueLease.t()) ::
          :ok | {:error, :lease_not_found | :lease_mismatch | :priority_index_migration_required}
  def release_queue_lease(repo, root, %QueueLease{} = lease) do
    ks = queue_lease_keyspace(root)

    with :ok <- require_queue_operation(repo, queue_keyspaces(root, lease.queue_id)) do
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

  A nonempty queue written before the scheduling index is held with
  `{:error, :priority_index_migration_required}` until an administrator runs
  the explicit writer-fenced migration. A genuinely empty queue is initialized
  atomically by its first enqueue. Only use that automatic bootstrap for a
  queue ID that no pre-index writer can subsequently target; otherwise fence
  old writers and migrate it explicitly before enqueuing. An unsupported or
  malformed v2 migration marker is held too, even if the raw queue is empty.

  Item vesting times and pointer activity timestamps use the unsigned 64-bit
  millisecond domain. An out-of-range `:now` returns
  `{:error, :vesting_time_out_of_range}` before queue state is written.

  Within a transaction:
  1. Writes item to queue zone with key {priority, vesting_time, id}
  2. Updates pointer index with atomic min for vesting_time
  3. Increments pending_count via atomic add
  """
  @spec enqueue(repo(), root_keyspace(), Item.t(), keyword()) ::
          :ok
          | {
              :error,
              :legacy_custom_id_unknown
              | :legacy_duplicate_custom_id
              | :priority_index_migration_required
              | timestamp_error()
            }
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
  established `:ok` return value. Nonempty pre-index queues return
  `{:error, :priority_index_migration_required}` until explicitly migrated.
  """
  @spec enqueue_with_item(repo(), root_keyspace(), Item.t(), keyword()) ::
          {:ok, Item.t()}
          | {
              :error,
              :legacy_custom_id_unknown
              | :legacy_duplicate_custom_id
              | :priority_index_migration_required
              | timestamp_error()
            }
  def enqueue_with_item(repo, root, %Item{} = item, opts \\ []) do
    Item.validate_priority!(item.priority)
    Item.validate_vesting_time!(item.vesting_time)
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now) || System.system_time(:millisecond)

    with :ok <- validate_timestamp(now),
         {:ok, priority_index_mode} <- initialize_empty_priority_index(repo, keyspaces) do
      identity_state = identity_state(repo, keyspaces)

      if custom_id?(item, opts) do
        enqueue_custom_id(repo, keyspaces, pointers, item, now, identity_state, priority_index_mode)
      else
        write_new_item(repo, keyspaces, pointers, item, now, priority_index_mode)
      end
    end
  end

  defp custom_id?(item, opts) do
    Keyword.get(opts, :custom_id?, Map.get(item, :custom_id?, false))
  end

  defp enqueue_custom_id(repo, keyspaces, pointers, item, now, identity_state, priority_index_mode) do
    case repo.get(keyspaces.identities, item.id) do
      nil ->
        enqueue_unindexed_custom_id(repo, keyspaces, pointers, item, now, identity_state, priority_index_mode)

      value ->
        {:ok, decode(value)}
    end
  end

  defp enqueue_unindexed_custom_id(repo, keyspaces, pointers, item, now, :current, priority_index_mode) do
    repo.put(keyspaces.identities, item.id, encode(item))
    write_new_item(repo, keyspaces, pointers, item, now, priority_index_mode)
  end

  defp enqueue_unindexed_custom_id(repo, keyspaces, _pointers, item, _now, :legacy, _priority_index_mode) do
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

  defp write_new_item(repo, keyspaces, pointers, item, now, priority_index_mode) do
    item_key = Item.key(item)
    repo.put(keyspaces.items, item_key, encode(item))

    case priority_index_mode do
      :bootstrap -> initialize_priority_index_for_first_item(repo, keyspaces.priority_index, item)
      :current -> add_item_to_priority_index(repo, keyspaces, item)
    end

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
  filtering them for visibility. A nonempty queue written before this index is
  held (returns `[]`) until an administrator starts an explicit writer-fenced
  migration. Check `priority_index_status/3` to distinguish this state from an
  empty queue. During a fenced migration it returns no jobs; only the explicit
  administrator migration call advances the bounded raw-item cursor.
  """
  @spec peek(repo(), root_keyspace(), String.t(), keyword()) :: [Item.t()]
  def peek(repo, root, queue_id, opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)
    limit = Keyword.get(opts, :limit, 10)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    case priority_index_state(keyspaces, repo) do
      :writer_fence_required -> []
      :migrating -> []
      :empty -> []
      :ready -> peek_ready_items(repo, keyspaces, limit, now)
    end
  end

  @doc false
  @spec migration_in_progress?(repo(), root_keyspace(), String.t()) :: boolean()
  def migration_in_progress?(repo, root, queue_id) do
    priority_index_status(repo, root, queue_id) == :migrating
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
          {:ok, Lease.t()}
          | {:error, :already_leased | :not_found | :priority_index_migration_required | timestamp_error()}
  def obtain_lease(repo, root, %Item{} = item, holder, duration_ms, opts \\ []) do
    keyspaces = queue_keyspaces(root, item.queue_id)
    pointers = pointer_keyspace(root)
    clock = clock(opts)

    with :ok <- require_queue_operation(repo, keyspaces) do
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
            with {:ok, lease_expires_at} <- future_vesting_time(now, duration_ms) do
              do_obtain_lease(repo, keyspaces, pointers, current_item, holder, duration_ms, now, lease_expires_at)
            end
          end
      end
    end
  end

  defp do_obtain_lease(repo, keyspaces, pointers, current_item, holder, duration_ms, now, lease_expires_at) do
    lease = Lease.new(current_item, holder, duration_ms: duration_ms, now: now)
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
    replace_item_in_priority_index(repo, keyspaces, current_item, updated_item)

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
  - `{:error, :vesting_time_out_of_range}` - Extending would exceed the
    unsigned 64-bit millisecond timestamp domain
  """
  @spec extend_lease(repo(), root_keyspace(), Lease.t(), pos_integer(), keyword()) ::
          {:ok, Lease.t()}
          | {
              :error,
              :lease_not_found
              | :lease_mismatch
              | :lease_expired
              | :item_not_found
              | :priority_index_migration_required
              | timestamp_error()
            }
  def extend_lease(repo, root, %Lease{} = lease, extension_ms, opts \\ []) do
    clock = clock(opts)
    keyspaces = queue_keyspaces(root, lease.queue_id)

    with :ok <- require_queue_operation(repo, keyspaces) do
      if lease.expires_at <= clock.() do
        {:error, :lease_expired}
      else
        case verify_active_lease(repo, keyspaces, lease, clock) do
          {:ok, stored_lease, _now} ->
            do_extend_lease(repo, root, keyspaces, stored_lease, extension_ms, clock)

          error ->
            error
        end
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
        with {:ok, now} <- active_now(stored_lease, clock),
             {:ok, new_expires_at} <- future_vesting_time(now, extension_ms) do
          item = decode(item_value)
          updated_item = %{item | vesting_time: new_expires_at, lease_expires_at: new_expires_at}

          # Delete old item key, write with new vesting_time
          repo.clear(keyspaces.items, old_item_key)
          new_item_key = Item.key(updated_item)
          repo.put(keyspaces.items, new_item_key, encode(updated_item))
          replace_item_in_priority_index(repo, keyspaces, item, updated_item)

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
          :ok | {:error, :lease_not_found | :lease_mismatch | :lease_expired | :priority_index_migration_required}
  def complete(repo, root, %Lease{} = lease, opts \\ []) do
    keyspaces = queue_keyspaces(root, lease.queue_id)
    clock = clock(opts)

    with :ok <- require_queue_operation(repo, keyspaces),
         {:ok, stored_lease, _now} <- verify_active_lease(repo, keyspaces, lease, clock),
         {:ok, _now} <- active_now(stored_lease, clock) do
      item_key = stored_lease.item_key
      repo.clear(keyspaces.items, item_key)
      repo.clear(keyspaces.leases, lease.item_id)
      remove_item_from_priority_index(repo, keyspaces, elem(stored_lease.item_key, 0), item_key)

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
  - `{:error, :vesting_time_out_of_range}` - Requeue backoff would exceed the
    unsigned 64-bit millisecond timestamp domain
  """
  @spec requeue(repo(), root_keyspace(), Lease.t(), keyword()) ::
          {:ok, :requeued | :dead_lettered}
          | {:error,
             :lease_not_found
             | :lease_mismatch
             | :lease_expired
             | :item_not_found
             | :priority_index_migration_required
             | timestamp_error()}
  def requeue(repo, root, %Lease{} = lease, opts) do
    keyspaces = queue_keyspaces(root, lease.queue_id)
    pointers = pointer_keyspace(root)
    clock = clock(opts)

    with :ok <- require_queue_operation(repo, keyspaces),
         {:ok, stored_lease, _now} <- verify_active_lease(repo, keyspaces, lease, clock),
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
    with {:ok, new_vesting_time} <- future_vesting_time(now, delay) do
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
      replace_item_in_priority_index(repo, keyspaces, item, updated_item)

      update_pointer(repo, pointers, new_vesting_time, lease.queue_id, now)
      repo.clear(keyspaces.leases, lease.item_id)
      update_stats(repo, keyspaces, 1, -1)

      {:ok, :requeued}
    end
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
  rows. A queue without a complete index, including one whose explicit offline
  migration is in progress, returns `{:error,
  :priority_index_migration_required}` rather than pretending to be empty or
  returning a partial minimum.
  """
  @spec min_vesting_time(repo(), root_keyspace(), String.t(), keyword()) ::
          non_neg_integer() | nil | {:error, :priority_index_migration_required}
  def min_vesting_time(repo, root, queue_id, _opts \\ []) do
    keyspaces = queue_keyspaces(root, queue_id)

    case priority_index_state(keyspaces, repo) do
      :writer_fence_required -> {:error, :priority_index_migration_required}
      :migrating -> {:error, :priority_index_migration_required}
      :empty -> nil
      :ready -> priority_index_minimum(repo, keyspaces)
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
          :ok | {:error, :priority_index_migration_required | timestamp_error()}
  def update_queue_pointer(repo, root, queue_id, vesting_time, opts \\ []) do
    pointers = pointer_keyspace(root)
    now = Keyword.get(opts, :now) || System.system_time(:millisecond)

    with :ok <- validate_timestamp(vesting_time),
         :ok <- validate_timestamp(now),
         :ok <- require_queue_operation(repo, queue_keyspaces(root, queue_id)) do
      # If new vesting_time is in the future, clean up any stale pointers in the past
      # This prevents the scanner from repeatedly finding stale pointers that point
      # to queues where all visible items have been processed
      if vesting_time > now do
        cleanup_past_pointers(repo, pointers, queue_id, now)
      end

      update_pointer(repo, pointers, vesting_time, queue_id, now)
      :ok
    end
  end

  # Cleans up pointers for a queue_id that are in the past (vesting_time <= now).
  # This is called when updating to a future vesting_time to remove stale pointers.
  defp cleanup_past_pointers(repo, pointers, queue_id, now) do
    {start_key, end_key} = pointer_visible_range(pointers, now)
    prefix = Keyspace.prefix(pointers)

    {start_key, end_key}
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

    # Range from 0 through now (inclusive).
    {start_key, end_key} = pointer_visible_range(pointers, now)
    prefix = Keyspace.prefix(pointers)

    {start_key, end_key}
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
    {start_key, end_key} = pointer_visible_range(pointers, cutoff)
    prefix = Keyspace.prefix(pointers)

    stale_pointers =
      {start_key, end_key}
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
    priority_index_state(keyspaces, repo) == :empty
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

  # All item and pointer timestamps share the tuple encoder's unsigned 64-bit
  # domain. Check an addition before producing a new item key so a lease or
  # retry cannot leave partial writes behind when it would overflow.
  defp future_vesting_time(now, delay), do: Item.add_vesting_time(now, delay)

  defp validate_timestamp(timestamp) do
    case Item.add_vesting_time(timestamp, 0) do
      {:ok, _timestamp} -> :ok
      {:error, :vesting_time_out_of_range} = error -> error
    end
  end

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
    remove_item_from_priority_index(repo, keyspaces, item.priority, item_key)
    update_stats(repo, keyspaces, 0, -1)
  end

  # Priority index

  # The index is two fixed-height binary min-trees: negative priorities come
  # first, then non-negative priorities. Their root values are folded into one
  # queue minimum. This preserves the full priority range accepted by item keys
  # while keeping every current-format lookup bounded.

  defp priority_index_present?(repo, keyspaces), do: not is_nil(priority_index_minimum(repo, keyspaces))

  # A v2 root is exact only after its initialized marker has been written. The
  # unversioned v1 tree is intentionally not consulted: an older writer cannot
  # observe a new marker, so raw legacy work remains held until an explicitly
  # writer-fenced administrator builds and activates v2.
  defp priority_index_state(keyspaces, repo) do
    priority_index_state(keyspaces, repo, migration_state(repo, keyspaces.priority_index))
  end

  defp priority_index_state(keyspaces, repo, migration_state) do
    index = keyspaces.priority_index

    case migration_state do
      {:offline_building, cursor} when is_nil(cursor) or is_binary(cursor) ->
        :migrating

      nil ->
        initialized_priority_index_state(repo, keyspaces, index)

      # An unknown v2 marker is not assumed to identify a coherent partial
      # tree. It remains held rather than reusing potentially stale values.
      _unknown_marker ->
        :writer_fence_required
    end
  end

  defp initialized_priority_index_state(repo, keyspaces, index) do
    if priority_index_initialized?(repo, index) do
      if priority_index_present?(repo, keyspaces), do: :ready, else: :empty
    else
      :writer_fence_required
    end
  end

  # Initializing a truly empty queue is safe and ergonomic: the bounded empty
  # range read and v2 marker write are in the caller's transaction. A nonempty
  # marker-less queue is held instead of guessing that no old writer exists.
  defp initialize_empty_priority_index(repo, keyspaces) do
    migration_state = migration_state(repo, keyspaces.priority_index)
    initialize_empty_priority_index_state(repo, keyspaces, migration_state)
  end

  defp initialize_empty_priority_index_state(repo, keyspaces, migration_state) do
    case priority_index_state(keyspaces, repo, migration_state) do
      :writer_fence_required when is_nil(migration_state) -> initialize_empty_v2_priority_index(repo, keyspaces)
      :writer_fence_required -> {:error, :priority_index_migration_required}
      :migrating -> {:error, :priority_index_migration_required}
      _current -> {:ok, :current}
    end
  end

  defp initialize_empty_v2_priority_index(repo, keyspaces) do
    if keyspace_has_entries?(repo, keyspaces.items) do
      {:error, :priority_index_migration_required}
    else
      put_priority_index_initialized(repo, keyspaces.priority_index)
      {:ok, :bootstrap}
    end
  end

  defp advance_offline_priority_migration(repo, keyspaces, {:offline_building, cursor}),
    do: advance_priority_migration(repo, keyspaces, cursor)

  defp advance_priority_migration(repo, keyspaces, cursor) do
    {start_key, end_key} = migration_item_range(keyspaces.items, cursor)

    rows =
      {start_key, end_key}
      |> repo.get_range(limit: @migration_chunk_size)
      |> Enum.to_list()

    Enum.each(rows, &merge_migrated_item(repo, keyspaces, &1))

    if length(rows) < @migration_chunk_size do
      complete_priority_index_migration(repo, keyspaces)
    else
      {last_key, _value} = List.last(rows)
      put_migration_state(repo, keyspaces.priority_index, last_key)
      :more
    end
  end

  defp complete_priority_index_migration(repo, keyspaces) do
    index = keyspaces.priority_index
    repo.clear(index, @priority_index_migration_key)
    put_priority_index_initialized(repo, index)
    priority_index_state(keyspaces, repo)
  end

  defp migration_item_range(item_keyspace, nil) do
    {start_key, end_key} =
      item_keyspace
      |> Keyspace.prefix()
      |> Bedrock.KeyRange.from_prefix()

    {start_key, end_key}
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

  defp priority_index_initialized?(repo, index), do: repo.get(index, @priority_index_initialized_key) == "ready"

  defp put_priority_index_initialized(repo, index), do: repo.put(index, @priority_index_initialized_key, "ready")

  defp migration_state(repo, index) do
    case repo.get(index, @priority_index_migration_key) do
      nil -> nil
      value -> decode_migration_state(value)
    end
  end

  # Migration state is control-plane data read before any queue operation.
  # Deserialize it with :safe so arbitrary persisted bytes cannot create atoms,
  # and collapse invalid or unsupported values to one fenced sentinel. A
  # present marker must never be mistaken for an absent marker.
  defp decode_migration_state(value) when is_binary(value) do
    value
    |> :erlang.binary_to_term([:safe])
    |> normalize_migration_state()
  rescue
    ArgumentError -> :invalid_migration_marker
  end

  defp decode_migration_state(_value), do: :invalid_migration_marker

  defp normalize_migration_state({:offline_building, cursor}), do: {:offline_building, cursor}
  defp normalize_migration_state(_other_marker), do: :invalid_migration_marker

  defp put_migration_state(repo, index, cursor),
    do: repo.put(index, @priority_index_migration_key, encode({:offline_building, cursor}))

  # Direct Store mutations (other than enqueue's separately conflict-checked
  # empty bootstrap) are allowed only once v2 is complete or known empty. This
  # one gate protects leases and pointers from markerless legacy queues and
  # unsupported v2 markers.
  defp require_queue_operation(repo, keyspaces) do
    case priority_index_state(keyspaces, repo) do
      status when status in [:ready, :empty] -> :ok
      _fenced_status -> {:error, :priority_index_migration_required}
    end
  end

  # The empty-range read immediately before this function conflict-tracks the
  # raw item namespace. The fresh v2 namespace has no legacy tree to clear or
  # read, so the first current-format item builds both fixed-height trees with
  # writes only. Sparse absent nodes exactly represent every other priority and
  # timestamp range.
  defp initialize_priority_index_for_first_item(repo, index, item) do
    {sign, leaf} = priority_location(item.priority)
    vesting_time = vesting_time!(item.vesting_time)

    repo.put(index, priority_member_key(sign, leaf, vesting_time, item.id), "indexed")
    put_priority_vesting_count(repo, index, sign, leaf, vesting_time, 1)

    for level <- (@vesting_bits - 1)..0//-1 do
      node = vesting_time >>> (@vesting_bits - level)
      put_priority_vesting_node(repo, index, sign, leaf, level, node, vesting_time)
    end

    for level <- @priority_bits..0//-1 do
      node = leaf >>> (@priority_bits - level)
      put_priority_node(repo, index, {sign, level, node}, item.vesting_time)
    end

    put_priority_node(repo, index, {"root"}, item.vesting_time)
  end

  defp merge_priority_index(repo, keyspaces, item) do
    add_item_to_priority_index(repo, keyspaces, item)
  end

  defp priority_location(priority)
       when is_integer(priority) and priority >= @min_priority and priority <= @max_priority do
    if priority < 0, do: {0, priority - @min_priority}, else: {1, priority}
  end

  defp priority_location(priority) do
    raise ArgumentError,
          "priority must be an integer between #{@min_priority} and #{@max_priority}, got: #{inspect(priority)}"
  end

  # Each active raw item contributes one membership row and one count at its
  # {priority, vesting_time} leaf. The fixed-height vesting tree gives that
  # priority's exact earliest timestamp without rereading the raw item range.
  # This makes a transaction correct even when its first raw range row was
  # locally cleared and the underlying range stream reports an empty page with
  # more storage rows behind it.
  defp add_item_to_priority_index(repo, keyspaces, item) do
    index = keyspaces.priority_index
    {sign, priority_leaf} = priority_location(item.priority)
    vesting_time = vesting_time!(item.vesting_time)
    member_key = priority_member_key(sign, priority_leaf, vesting_time, item.id)

    if is_nil(repo.get(index, member_key)) do
      repo.put(index, member_key, "indexed")
      adjust_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, 1)
      refresh_priority_vesting_path(repo, index, sign, priority_leaf, vesting_time)
      refresh_global_priority_from_vesting(repo, index, sign, priority_leaf)
    end
  end

  defp replace_item_in_priority_index(repo, keyspaces, old_item, new_item) do
    index = keyspaces.priority_index

    changed_locations =
      Enum.reject(
        [remove_priority_index_member(repo, index, old_item), add_priority_index_member(repo, index, new_item)],
        &is_nil/1
      )

    refresh_changed_priority_locations(repo, index, changed_locations)
  end

  defp remove_item_from_priority_index(repo, keyspaces, priority, item_key) do
    {_priority, vesting_time, item_id} = item_key
    index = keyspaces.priority_index
    item = %{priority: priority, vesting_time: vesting_time, id: item_id}

    case remove_priority_index_member(repo, index, item) do
      nil -> :ok
      location -> refresh_changed_priority_locations(repo, index, [location])
    end
  end

  defp add_priority_index_member(repo, index, item) do
    {sign, priority_leaf} = priority_location(item.priority)
    vesting_time = vesting_time!(item.vesting_time)
    member_key = priority_member_key(sign, priority_leaf, vesting_time, item.id)

    if is_nil(repo.get(index, member_key)) do
      repo.put(index, member_key, "indexed")
      adjust_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, 1)
      {sign, priority_leaf, vesting_time}
    end
  end

  defp remove_priority_index_member(repo, index, item) do
    {sign, priority_leaf} = priority_location(item.priority)
    vesting_time = vesting_time!(item.vesting_time)
    member_key = priority_member_key(sign, priority_leaf, vesting_time, item.id)

    if repo.get(index, member_key) do
      repo.clear(index, member_key)
      adjust_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, -1)
      {sign, priority_leaf, vesting_time}
    end
  end

  defp refresh_changed_priority_locations(repo, index, changed_locations) do
    changed_locations
    |> Enum.uniq()
    |> Enum.each(fn {sign, priority_leaf, vesting_time} ->
      refresh_priority_vesting_path(repo, index, sign, priority_leaf, vesting_time)
    end)

    changed_locations
    |> Enum.map(fn {sign, priority_leaf, _vesting_time} -> {sign, priority_leaf} end)
    |> Enum.uniq()
    |> Enum.each(fn {sign, priority_leaf} ->
      refresh_global_priority_from_vesting(repo, index, sign, priority_leaf)
    end)
  end

  defp adjust_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, delta) do
    current_count = priority_vesting_count(repo, index, sign, priority_leaf, vesting_time)
    updated_count = current_count + delta

    if updated_count < 0 do
      raise ArgumentError,
            "priority index member count cannot become negative for priority leaf #{priority_leaf} at #{vesting_time}"
    end

    put_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, updated_count)
  end

  defp refresh_priority_vesting_path(repo, index, sign, priority_leaf, vesting_time) do
    refresh_priority_vesting_ancestors(
      repo,
      index,
      sign,
      priority_leaf,
      @vesting_bits - 1,
      div(vesting_time, 2)
    )
  end

  defp refresh_priority_vesting_ancestors(repo, index, sign, priority_leaf, level, node) do
    left = priority_vesting_minimum(repo, index, sign, priority_leaf, level + 1, node * 2)
    right = priority_vesting_minimum(repo, index, sign, priority_leaf, level + 1, node * 2 + 1)

    put_priority_vesting_node(repo, index, sign, priority_leaf, level, node, minimum(left, right))

    if level > 0 do
      refresh_priority_vesting_ancestors(repo, index, sign, priority_leaf, level - 1, div(node, 2))
    end
  end

  defp refresh_global_priority_from_vesting(repo, index, sign, priority_leaf) do
    minimum = priority_vesting_minimum(repo, index, sign, priority_leaf, 0, 0)

    put_priority_node(repo, index, {sign, @priority_bits, priority_leaf}, minimum)
    refresh_priority_ancestors(repo, index, sign, @priority_bits - 1, div(priority_leaf, 2))
    refresh_priority_root(repo, index)
  end

  defp priority_member_key(sign, priority_leaf, vesting_time, item_id),
    do: {"member", sign, priority_leaf, vesting_time, item_id}

  defp priority_vesting_key(sign, priority_leaf, level, node), do: {"vesting", sign, priority_leaf, level, node}

  defp priority_vesting_count(repo, index, sign, priority_leaf, vesting_time) do
    case repo.get(index, priority_vesting_key(sign, priority_leaf, @vesting_bits, vesting_time)) do
      nil -> 0
      value -> decode_timestamp(value)
    end
  end

  defp put_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, 0),
    do: repo.clear(index, priority_vesting_key(sign, priority_leaf, @vesting_bits, vesting_time))

  defp put_priority_vesting_count(repo, index, sign, priority_leaf, vesting_time, count),
    do: repo.put(index, priority_vesting_key(sign, priority_leaf, @vesting_bits, vesting_time), encode_timestamp(count))

  defp priority_vesting_minimum(repo, index, sign, priority_leaf, @vesting_bits, vesting_time) do
    if priority_vesting_count(repo, index, sign, priority_leaf, vesting_time) == 0, do: nil, else: vesting_time
  end

  defp priority_vesting_minimum(repo, index, sign, priority_leaf, level, node) do
    case repo.get(index, priority_vesting_key(sign, priority_leaf, level, node)) do
      nil -> nil
      value -> decode_timestamp(value)
    end
  end

  defp put_priority_vesting_node(repo, index, sign, priority_leaf, level, node, nil),
    do: repo.clear(index, priority_vesting_key(sign, priority_leaf, level, node))

  defp put_priority_vesting_node(repo, index, sign, priority_leaf, level, node, vesting_time),
    do: repo.put(index, priority_vesting_key(sign, priority_leaf, level, node), encode_timestamp(vesting_time))

  defp vesting_time!(vesting_time), do: Item.validate_vesting_time!(vesting_time)

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

  defp pointer_visible_range(pointers, now) when is_integer(now) do
    prefix = Keyspace.prefix(pointers)
    start_key = prefix <> TupleEncoding.pack({0, <<>>})

    cond do
      now < 0 ->
        {start_key, start_key}

      now >= Item.max_vesting_time() ->
        {start_key, prefix_end(prefix)}

      true ->
        {start_key, prefix <> TupleEncoding.pack({now + 1, <<>>})}
    end
  end

  defp pointer_visible_range(pointers, _now) do
    prefix = Keyspace.prefix(pointers)
    start_key = prefix <> TupleEncoding.pack({0, <<>>})
    {start_key, start_key}
  end

  defp prefix_end(prefix), do: prefix |> Bedrock.KeyRange.from_prefix() |> elem(1)

  defp unpack_pointer_key(suffix) do
    TupleEncoding.unpack(suffix)
  end
end
