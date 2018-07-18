defmodule RabbitMQ.Consumer do
  @moduledoc """
  RabbitMQ consumer that processess messages concurrently.

  The concurrency level of the consumer is based on the `prefetch_count` option.
  The consumer should specify the processing timeout. When the timeout is reached,
  the processing is terminated and the message is rejected.

  The specified queue is declared when the consumer is started. An additional dead-letter
  queue is declared as well. If message processing fails, the message is requeued
  and retried. Upon second failure, the message is sent to the dead-letter exchange
  and routed to the dead-letter queue.

  The consumer automatically reconnects to the server when the connection is lost.

  The consumer adds the `:message_tag` field to the Logger metadata of the consumer process.

  The consumer uses the name of the callback module as a basis for name registration of the
  relevant child OTP processes for easier inspection.

  ## Example

      defmodule MyConsumer do
        def init() do
          # perform initialization here
          # start_link() will block until init() completes
        end

        def consume(payload) do
          # perform the processing: the function runs in a separate process,
          # the message will be ack-ed if it exits normally
          # and rejected otherwise
        end
      end

      iex> opts = [rabbitmq_url: ...]
      iex> RabbitMQ.Consumer.start_link(MyConsumer, opts)
  """

  use Supervisor

  require Logger

  @doc """
  Starts and links the RabbitMQ consumer process with `callback_mod` as the callback module.

  The callback module is expected to implement two functions: `init()` and `consume(payload)`.

  ## Options

    * `:rabbitmq_url` - the URL of the RabbitMQ server, default is "amqp://guest:guest@localhost:5672"

    * `:queue` - name of the consumer queue

    * `:exchange` - name of the exchange to which the queue should bind;
      if specified multiple times, the queue will bind to all specified exchanges

    * `:routing_key` - the message routing key to use when binding the queue;
      default is "#"

    * `:prefetch_count` - message prefetch count; this is equivalent to the maximum concurrency
      level of processing; default is 10

    * `:processing_timeout` - timeout for completing the processing of a message, default is 10 seconds
  """
  def start_link(callback_mod, opts) do
    Logger.info("Starting Consumer...")
    Supervisor.start_link(__MODULE__, [callback_mod, opts], name: callback_mod)
  end

  def init([callback_mod, opts]) do
    children = [
      {Task.Supervisor, name: task_supervisor_name(callback_mod)},
      {RabbitMQ.Client, [callback_mod, opts]}
    ]

    Supervisor.init(children, strategy: :one_for_all, name: callback_mod)
  end

  def client_name(callback_mod) do
    :"#{callback_mod}.Client"
  end

  def task_supervisor_name(callback_mod) do
    :"#{callback_mod}.TaskSupervisor"
  end
end
