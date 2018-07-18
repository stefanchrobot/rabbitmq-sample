defmodule Sample do
  def publish(payload, num \\ 1) do
    {:ok, conn} = AMQP.Connection.open("amqp://guest:guest@localhost:5672")
    {:ok, chan} = AMQP.Channel.open(conn)

    Enum.each(1..num, fn _ ->
      AMQP.Basic.publish(chan, "sample_exchange", "", payload, persistent: true)
    end)
  end

  def test(num \\ 1) do
    if num == 1 do
      publish("test")
    else
      spawn(fn -> publish("test", num) end)
    end
  end

  def long(num \\ 1), do: publish("long", num)
  def crash(), do: publish("crash")
  def kill(), do: publish("kill")
  def shutdown(), do: publish("shutdown")
end
