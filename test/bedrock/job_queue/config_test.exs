defmodule Bedrock.JobQueue.ConfigTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Config

  describe "new/1" do
    test "creates config with required repo" do
      config = Config.new(repo: MyApp.Repo)

      assert config.repo == MyApp.Repo
      assert config.concurrency == System.schedulers_online()
      assert config.batch_size == 10
    end

    test "creates config with custom options" do
      config = Config.new(repo: MyApp.Repo, concurrency: 4, batch_size: 20)

      assert config.repo == MyApp.Repo
      assert config.concurrency == 4
      assert config.batch_size == 20
    end
  end

  describe "default_backoff/1" do
    test "returns exponential backoff with jitter for attempt 0" do
      delay = Config.default_backoff(0)
      assert delay >= 1000 and delay <= 1500
    end

    test "returns exponential backoff with jitter for attempt 3" do
      delay = Config.default_backoff(3)
      assert delay >= 8000 and delay <= 8500
    end
  end
end
