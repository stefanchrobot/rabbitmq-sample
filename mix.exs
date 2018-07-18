defmodule Rmq.MixProject do
  use Mix.Project

  def project do
    [
      app: :sample,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Sample.Application, []},
      # https://github.com/pma/amqp/wiki/Upgrade-from-0.X-to-1.0#lager
      extra_applications: [:lager, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amqp, "~> 1.0.3"}
    ]
  end
end
