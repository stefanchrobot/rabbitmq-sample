defmodule Sample.Application do
  use Application

  require Logger

  def start(_type, _args) do
    Logger.info("Starting Sample.Application...")
    Supervisor.start_link([Sample.Consumer], strategy: :one_for_one, name: __MODULE__)
  end
end
