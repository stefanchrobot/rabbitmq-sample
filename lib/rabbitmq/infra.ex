defmodule RabbitMQ.Infra do
  @moduledoc """
  Functions for setting up RabbitMQ exchanges and queues.
  """

  require Logger

  @doc """
  Connects to a RabbitMQ server.

  The function blocks until a successfult connection is established, trying every second.
  """
  def connect(rabbitmq_url) do
    Logger.info("Connecting to RabbitMQ at #{rabbitmq_url}...")

    case AMQP.Connection.open(rabbitmq_url) do
      {:ok, conn} ->
        {:ok, chan} = AMQP.Channel.open(conn)
        Logger.info("Connection to RabbitMQ at #{rabbitmq_url} established")
        {:ok, {conn, chan}}

      {:error, details} ->
        Logger.warn("Failed to connect to RabbitMQ at #{rabbitmq_url}: #{inspect(details)}")
        :timer.sleep(3000)
        connect(rabbitmq_url)
    end
  end

  @doc """
  Sets up a queue and binds it to the specified exchanges.

  Additionally, sets up a dead-letter exchange and queue.

  ## Options

    * `:queue` - name of the consumer queue

    * `:exchange` - name of the exchange to which the queue should bind;
      if specified multiple times, the queue will bind to all specified exchanges

    * `:routing_key` - the message routing key to use when binding the queue;
      default is "#"

    * `:prefetch_count` - message prefetch count; default 10
  """
  def setup_queue(chan, opts) do
    queue = Keyword.fetch!(opts, :queue)
    exchanges = Keyword.get_values(opts, :exchange)
    routing_key = Keyword.get(opts, :routing_key, "#")
    dead_letter_exchange = dead_letter_exchange(queue)
    dead_letter_queue = dead_letter_queue(queue)

    # Error exchange and error queue
    :ok = AMQP.Exchange.declare(chan, dead_letter_exchange, :fanout)
    {:ok, _map} = AMQP.Queue.declare(chan, dead_letter_queue, durable: true)
    :ok = AMQP.Queue.bind(chan, dead_letter_queue, dead_letter_exchange)

    Logger.info("Declared exchange '#{dead_letter_exchange}'")
    Logger.info("Declared queue '#{dead_letter_queue}'")
    Logger.info("Bound queue '#{dead_letter_queue}' to exchange '#{dead_letter_exchange}'")

    # Queue
    {:ok, _map} =
      AMQP.Queue.declare(
        chan,
        queue,
        durable: true,
        arguments: [
          {"x-dead-letter-exchange", :longstr, dead_letter_exchange}
        ]
      )

    Logger.info("Declared queue '#{queue}' with dead-letter exchange '#{dead_letter_exchange}'")

    Enum.each(exchanges, fn exchange ->
      :ok = AMQP.Queue.bind(chan, queue, exchange, routing_key: routing_key)
      Logger.info("Bound queue '#{queue}' to exchange '#{exchange}'")
    end)

    # Limit unacknowledged messages
    prefetch_count = Keyword.get(opts, :prefetch_count, 10)
    :ok = AMQP.Basic.qos(chan, prefetch_count: prefetch_count)
    Logger.info("Set prefetch_count for queue '#{queue}' to #{prefetch_count}")

    :ok
  end

  @doc """
  Returns the name of the dead letter exchange for a specified queue.
  """
  def dead_letter_exchange(queue) do
    "#{queue}_dlx"
  end

  @doc """
  Returns the name of the dead letter queue for a specified queue.
  """
  def dead_letter_queue(queue) do
    "#{queue}_dlq"
  end
end
