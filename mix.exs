defmodule BedrockJobQueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock_job_queue,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: dialyzer(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.json": :test,
        "coveralls.github": :test,
        dialyzer: :dev
      ]
    ]
  end

  defp aliases, do: [quality: ["format --check-formatted", "credo --strict", "dialyzer"]]

  defp dialyzer do
    [
      plt_core_path: "priv/plts",
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      plt_add_apps: [:ex_unit, :mix],
      # Disable opaque type checks due to OTP 28 issues with structs containing
      # MapSet/queue. See: https://github.com/elixir-lang/elixir/issues/14576
      flags: [:no_opaque]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bedrock, "~> 0.4"},
      {:mox, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.39", only: :dev, runtime: false, warn_if_outdated: true},
      {:styler, "~> 1.0", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.7", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp description do
    "Durable job queue for Bedrock - transactional, priority-ordered job processing modeled after Apple's QuiCK."
  end

  defp package do
    [
      name: "bedrock_job_queue",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/bedrock-kv/job_queue"},
      maintainers: ["Jason Allum"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
