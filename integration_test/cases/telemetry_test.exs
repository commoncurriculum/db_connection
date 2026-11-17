defmodule TelemetryTest do
  use ExUnit.Case, async: false

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  @events [[:db_connection, :checkout], [:db_connection, :checkin]]

  defp attach(pool) do
    test = self()
    id = make_ref()

    :telemetry.attach_many(
      id,
      @events,
      fn event, measurements, metadata, _ ->
        if metadata.pool == pool, do: send(test, {event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  test "an execute checks a connection out and back in, with the pool's label" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:ok, %Q{}, %R{}, :state}])
    {:ok, pool} = P.start_link(agent: agent, parent: self(), label: :my_pool)
    attach(pool)

    assert {:ok, %Q{}, %R{}} = P.execute(pool, %Q{}, [:param])

    assert_receive {[:db_connection, :checkout], %{wait_time: wait_time, idle_time: idle_time},
                    %{pool: ^pool, label: :my_pool, connection_module: TestConnection}}

    assert wait_time >= 0
    assert is_nil(idle_time) or idle_time >= 0

    assert_receive {[:db_connection, :checkin], %{use_time: use_time},
                    %{pool: ^pool, label: :my_pool, connection_module: TestConnection}}

    assert use_time >= 0
    refute_received {[:db_connection, _], _, _}
  end

  test "use time covers the whole checkout" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])
    {:ok, pool} = P.start_link(agent: agent, parent: self())
    attach(pool)

    P.run(pool, fn _conn -> Process.sleep(50) end)

    assert_receive {[:db_connection, :checkout], _, %{label: nil}}
    assert_receive {[:db_connection, :checkin], %{use_time: use_time}, %{label: nil}}
    assert System.convert_time_unit(use_time, :native, :millisecond) >= 50
  end

  test "wait time covers the time spent queued for a busy connection" do
    {:ok, agent} = A.start_link([{:ok, :state} | List.duplicate({:idle, :state}, 4)])
    {:ok, pool} = P.start_link(agent: agent, parent: self())
    attach(pool)
    test = self()

    Task.start(fn ->
      Process.put(:agent, agent)

      P.run(pool, fn _conn ->
        send(test, :holding)
        Process.sleep(50)
      end)
    end)

    assert_receive :holding
    P.run(pool, fn _conn -> :ok end)

    assert_receive {[:db_connection, :checkout], _, _}
    assert_receive {[:db_connection, :checkin], _, _}
    assert_receive {[:db_connection, :checkout], %{wait_time: wait_time}, _}
    assert System.convert_time_unit(wait_time, :native, :millisecond) >= 40
  end

  test "a transaction is one checkout and one checkin" do
    {:ok, agent} =
      A.start_link([
        {:ok, :state},
        {:ok, :began, :state},
        {:ok, %Q{}, %R{}, :state},
        {:ok, :committed, :state}
      ])

    {:ok, pool} = P.start_link(agent: agent, parent: self())
    attach(pool)

    assert {:ok, %R{}} =
             P.transaction(pool, fn conn -> P.execute!(conn, %Q{}, [:param]) end)

    assert_receive {[:db_connection, :checkout], _, _}
    assert_receive {[:db_connection, :checkin], _, _}
    refute_received {[:db_connection, _], _, _}
  end

  test "a checkout that times out in the queue is not a checkout" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])

    {:ok, pool} =
      P.start_link(agent: agent, parent: self(), queue_target: 10, queue_interval: 10)

    attach(pool)

    P.run(pool, fn _conn ->
      assert_raise DBConnection.ConnectionError, fn ->
        P.run(pool, fn _ -> flunk("got connection") end, queue_target: 10, queue_interval: 10)
      end
    end)

    assert_receive {[:db_connection, :checkout], _, %{connection_module: TestConnection}}
    assert_receive {[:db_connection, :checkin], _, _}
    refute_received {[:db_connection, _], _, _}
  end
end
