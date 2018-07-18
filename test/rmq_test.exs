defmodule RmqTest do
  use ExUnit.Case
  doctest Rmq

  test "greets the world" do
    assert Rmq.hello() == :world
  end
end
