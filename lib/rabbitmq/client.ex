defmodule RabbitMQ.Client do
  @moduledoc """
  A RabbitMQ client.

  The client connects to the RabbitMQ server and sets up the consumer queue. The client
  should not be used directly, use `RabbitMQ.Consumer` instead.

  Acking or rejecting a message must happen on the same connection/channel as receiving
  the message. If the connection goes down, the client goes down too, which is useful
  for setting up a proper restart strategy in a supervision tree.
  """

  use GenServer

  require Logger

  alias RabbitMQ.{Consumer, Infra}

  @doc """
  Starts and links the client process.

  ## Options

    * `:rabbitmq_url` - the URL of the RabbitMQ server, default is "amqp://guest:guest@localhost:5672"

    * `:queue` - name of the consumer queue

    * `:exchange` - name of the exchange to which the queue should bind;
      if specified multiple times, the queue will bind to many exchanges

    * `:routing_key` - the message routing key to use when binding the queue;
      default is "#"

    * `:prefetch_count` - message prefetch count; this is equivalent of the maximum concurrency
      level of processing; default is 10

    * `:processing_timeout` - timeout for completing the processing of a message, default is 10 seconds
  """
  def start_link([callback_mod, opts]) do
    Logger.info("Starting Client...")

    GenServer.start_link(
      __MODULE__,
      [callback_mod, opts],
      name: Consumer.client_name(callback_mod)
    )
  end

  def init([callback_mod, opts]) do
    queue = Keyword.fetch!(opts, :queue)
    rabbitmq_url = Keyword.get(opts, :rabbitmq_url, "amqp://guest:guest@localhost:5672")

    {:ok, {conn, chan}} = Infra.connect(rabbitmq_url)
    Process.link(conn.pid)
    Process.link(chan.pid)

    :ok = Infra.setup_queue(chan, opts)
    # Register the GenServer process as a consumer
    {:ok, _consumer_tag} = AMQP.Basic.consume(chan, queue)
    state = %{conn: conn, chan: chan, callback_mod: callback_mod, opts: opts, tasks: %{}}

    {:ok, state}
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    Logger.warn("Consumer unexpectedly cancelled")
    {:stop, :normal, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    Logger.info("Got confirmation about cancelling consumption")
    {:noreply, state}
  end

  # Payload delivered to the consumer
  def handle_info(
        {:basic_deliver, payload, %{delivery_tag: tag, redelivered: redelivered?}},
        %{callback_mod: callback_mod, opts: opts, tasks: tasks} = state
      ) do
    task = consume(payload, tag, redelivered?, callback_mod)

    timer =
      Process.send_after(self(), {:kill_task, task.pid}, opts[:processing_timeout] || 10_000)

    {:noreply, %{state | tasks: Map.put(tasks, task.pid, {tag, redelivered?, timer})}}
  end

  # Task completed successfully
  def handle_info({_ref, {:consume_task_finished, pid}}, %{chan: chan, tasks: tasks} = state) do
    {tag, _redelivered?, timer} = tasks[pid]
    Process.cancel_timer(timer)
    :ok = AMQP.Basic.ack(chan, tag)
    Logger.debug("Acked message #{tag}")
    {:noreply, %{state | tasks: Map.delete(tasks, pid)}}
  end

  # Graceful task shutdown
  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
    # The task was already removed from state.tasks
    {:noreply, state}
  end

  # Processing timeout reached
  def handle_info({:kill_task, pid}, state) do
    Process.exit(pid, :processing_timeout)
    {:noreply, state}
  end

  # Linked/monitored process went down
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{chan: chan, tasks: tasks} = state) do
    case tasks[pid] do
      {tag, redelivered?, timer} ->
        Logger.warn("Error processing message #{tag}: #{inspect(reason)}")
        Process.cancel_timer(timer)
        # Requeue unless it's a redelivered message. This means we will retry
        # consuming the message before moving it to the dead-letter queue.
        :ok = AMQP.Basic.reject(chan, tag, requeue: not redelivered?)
        Logger.debug("Rejected message #{tag}, requeued? #{not redelivered?}")

      nil ->
        # Task completed successfully, or non-task
        nil
    end

    {:noreply, %{state | tasks: Map.delete(tasks, pid)}}
  end

  defp consume(payload, tag, redelivered?, callback_mod) do
    Logger.debug("Consuming message #{tag}, redelivered?: #{redelivered?}")

    Task.Supervisor.async_nolink(Consumer.task_supervisor_name(callback_mod), fn ->
      Logger.metadata(message_tag: tag)
      {elapsed, _value} = :timer.tc(callback_mod, :consume, [payload])
      elapsed_sec = :erlang.float_to_binary(elapsed / 1_000_000, decimals: 3)
      Logger.debug("Consumed message #{tag} in #{elapsed_sec}s")
      {:consume_task_finished, self()}
    end)
  end
end
