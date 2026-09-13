defmodule Bedrock.JobQueue.Consumer.Action do
  @moduledoc false

  alias Bedrock.Internal.Repo, as: InternalRepo
  alias Bedrock.JobQueue.Store

  @type action :: :complete | :requeue | {:snooze, non_neg_integer()}

  @spec run(module(), Bedrock.Keyspace.t(), Bedrock.JobQueue.Lease.t(), action(), term(), keyword()) :: term()
  def run(repo, root, lease, action, handler_result, opts) do
    repo.transact(fn -> run_transaction(repo, root, lease, action, handler_result, opts) end)
  end

  defp run_transaction(repo, root, lease, action, handler_result, opts) do
    queue_result = run_queue_action(repo, root, lease, action, opts)

    if queue_action_succeeded?(queue_result) do
      run_action_hook_or_rollback(
        Keyword.get(opts, :action_hook),
        repo,
        root,
        lease,
        action,
        handler_result,
        queue_result
      )
    else
      queue_result
    end
  end

  defp run_action_hook_or_rollback(action_hook, repo, root, lease, action, handler_result, queue_result) do
    case run_action_hook(action_hook, repo, root, lease, action, handler_result, queue_result) do
      :ok -> queue_result
      {:error, reason} -> repo.rollback({:action_hook_failed, reason})
    end
  end

  defp run_queue_action(repo, root, lease, :complete, _opts), do: Store.complete(repo, root, lease)

  defp run_queue_action(repo, root, lease, :requeue, opts) do
    Store.requeue(repo, root, lease, backoff_fn: Keyword.fetch!(opts, :backoff_fn))
  end

  defp run_queue_action(repo, root, lease, {:snooze, delay_ms}, _opts) do
    Store.requeue(repo, root, lease, base_delay: delay_ms, max_delay: delay_ms)
  end

  defp queue_action_succeeded?(:ok), do: true
  defp queue_action_succeeded?({:ok, _status}), do: true
  defp queue_action_succeeded?({:error, _reason}), do: false

  defp run_action_hook(nil, _repo, _root, _lease, _action, _handler_result, _queue_result), do: :ok

  defp run_action_hook(action_hook, repo, root, lease, action, handler_result, queue_result) do
    hook_args = [repo, root, lease, action, handler_result, queue_result]

    action_hook
    |> invoke_action_hook(hook_args)
    |> normalize_action_hook_result()
  end

  defp invoke_action_hook(action_hook, hook_args) do
    case action_hook do
      {module, function} -> apply(module, function, hook_args)
      {module, function, extra_args} when is_list(extra_args) -> apply(module, function, hook_args ++ extra_args)
    end
  rescue
    exception -> {:error, {:exception, exception}}
  catch
    :throw, {InternalRepo, :rollback, _reason} = rollback -> throw(rollback)
    :throw, reason -> {:error, {:throw, reason}}
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp normalize_action_hook_result(hook_result) do
    case hook_result do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_action_hook_return, other}}
    end
  end
end
