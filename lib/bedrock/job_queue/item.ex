defmodule Bedrock.JobQueue.Item do
  @moduledoc """
  A job item in the queue.

  ## Fields

  - `id` - Unique job identifier (UUID binary)
  - `custom_id?` - Whether the ID was supplied by the caller
  - `topic` - Job type/topic (Phoenix PubSub-style, e.g., "user:created")
  - `priority` - Integer priority (lower = higher priority)
  - `vesting_time` - When the job becomes visible (milliseconds since epoch)
  - `lease_id` - Current lease holder (nil if available)
  - `lease_expires_at` - When the lease expires
  - `error_count` - Number of failed attempts
  - `max_retries` - Maximum retry attempts
  - `payload` - Job-specific data (binary, typically JSON)
  - `queue_id` - The queue/tenant this job belongs to
  """

  alias Bedrock.JobQueue.Payload

  import Bitwise

  @type t :: %__MODULE__{
          id: binary(),
          custom_id?: boolean(),
          topic: String.t(),
          priority: integer(),
          vesting_time: non_neg_integer(),
          lease_id: binary() | nil,
          lease_expires_at: non_neg_integer() | nil,
          error_count: non_neg_integer(),
          max_retries: non_neg_integer(),
          payload: binary(),
          queue_id: String.t()
        }

  defstruct [
    :id,
    :topic,
    :priority,
    :vesting_time,
    :lease_id,
    :lease_expires_at,
    :error_count,
    :max_retries,
    :payload,
    :queue_id,
    custom_id?: false
  ]

  @default_priority 100
  @default_max_retries 3
  @max_priority (1 <<< 64) - 1
  @min_priority -@max_priority

  @doc false
  @spec min_priority() :: integer()
  def min_priority, do: @min_priority

  @doc false
  @spec max_priority() :: integer()
  def max_priority, do: @max_priority

  @doc false
  @spec validate_priority!(term()) :: integer()
  def validate_priority!(priority)
      when is_integer(priority) and priority >= @min_priority and priority <= @max_priority,
      do: priority

  def validate_priority!(priority) do
    raise ArgumentError,
          "priority must be an integer between #{@min_priority} and #{@max_priority}, got: #{inspect(priority)}"
  end

  @doc """
  Creates a new job item with defaults.

  ## Options

  - `:id` - Custom job ID (default: random 16-byte binary). A supplied ID is
    retained as enqueue intent so direct `Store.enqueue/4` calls are idempotent.
  - `:priority` - Integer priority, lower = higher priority (default: 100)
  - `:vesting_time` - When the job becomes visible in ms since epoch (default: now)
  - `:max_retries` - Maximum retry attempts before dead-lettering (default: 3)
  - `:now` - Current time in ms, used for vesting_time default (default: System.system_time(:millisecond))

  ## Priority Ordering

  Jobs are processed in priority order where **lower values = higher priority**.
  For example, priority -1 is processed before priority 0, which is processed
  before priority 100. Priorities are restricted to the tuple-key encoder's
  range, `-18_446_744_073_709_551_615..18_446_744_073_709_551_615`.
  """
  @spec new(String.t(), String.t(), term(), keyword()) :: t()
  def new(queue_id, topic, payload, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    priority = opts |> Keyword.get(:priority, @default_priority) |> validate_priority!()

    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      custom_id?: Keyword.has_key?(opts, :id),
      topic: topic,
      priority: priority,
      vesting_time: Keyword.get(opts, :vesting_time, now),
      lease_id: nil,
      lease_expires_at: nil,
      error_count: 0,
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      payload: Payload.encode(payload),
      queue_id: queue_id
    }
  end

  @doc """
  Returns true if the job is currently visible.

  An item is visible when its vesting time has passed and it is not actively
  leased. Expired leases are considered visible so another worker can reclaim
  stale work.
  """
  @spec visible?(t()) :: boolean()
  @spec visible?(t(), non_neg_integer()) :: boolean()
  def visible?(item, now \\ System.system_time(:millisecond))

  def visible?(%__MODULE__{vesting_time: vt, lease_id: nil}, now), do: now >= vt

  def visible?(%__MODULE__{vesting_time: vt, lease_expires_at: exp}, now)
      when not is_nil(exp) do
    now >= vt and now >= exp
  end

  def visible?(%__MODULE__{}, _now), do: false

  @doc """
  Returns true if the job is currently leased.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec leased?(t(), keyword()) :: boolean()
  def leased?(item, opts \\ [])

  def leased?(%__MODULE__{lease_id: nil}, _opts), do: false

  def leased?(%__MODULE__{lease_expires_at: exp}, opts) when not is_nil(exp) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    now < exp
  end

  def leased?(_, _opts), do: false

  @doc """
  Returns true if retries are exhausted.
  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{error_count: ec, max_retries: mr}), do: ec >= mr

  @doc """
  Builds the storage key tuple for this item.

  Keys are `{priority, vesting_time, id}` which sorts items by priority first,
  then by vesting time, then by unique id.
  """
  @spec key(t()) :: {integer(), non_neg_integer(), binary()}
  def key(%__MODULE__{priority: p, vesting_time: vt, id: id}), do: {p, vt, id}

  defp generate_id, do: :crypto.strong_rand_bytes(16)
end
