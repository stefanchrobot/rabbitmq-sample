defmodule Sample.Consumer do
  require Logger

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def opts() do
    [
      queue: "sample_queue",
      prefetch_count: 10,
      exchange: "sample_exchange",
      processing_timeout: 20_000
    ]
  end

  def start_link() do
    Logger.info("Starting Sample.Consumer...")
    RabbitMQ.Consumer.start_link(__MODULE__, opts())
  end

  def init() do
    # no initialization needed
  end

  def consume(payload) do
    case payload do
      "crash" ->
        Logger.debug("Sample.Consumer: crashing intentionally...")
        1 / 0

      "kill" ->
        Logger.debug("Sample.Consumer: exiting with :kill...")
        exit(:kill)

      "shutdown" ->
        Logger.debug("Sample.Consumer: exiting with :shutdown...")
        exit(:shutdown)

      "long" ->
        Logger.debug("Sample.Consumer: simulating long processing (30s)...")
        :timer.sleep(30000)

      _ ->
        Logger.debug("Sample.Consumer: regular message (1s)...")
        :timer.sleep(1000)
    end
  end
end
