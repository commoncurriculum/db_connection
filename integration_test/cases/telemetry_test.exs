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

    assert_receive {[:db_connection, :checkout], %{wait_time: wait_time} = measurements,
                    %{pool: ^pool, label: :my_pool, connection_module: TestConnection}}

    assert wait_time >= 0
    assert Map.keys(measurements) == [:wait_time]

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

  test "a checkout that times out in the queue is a timed-out checkout with its error" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])

    {:ok, pool} =
      P.start_link(
        agent: agent,
        parent: self(),
        label: :my_pool,
        queue_target: 10,
        queue_interval: 10
      )

    attach(pool)

    P.run(pool, fn _conn ->
      assert_raise DBConnection.ConnectionError, fn ->
        P.run(pool, fn _ -> flunk("got connection") end, queue_target: 10, queue_interval: 10)
      end
    end)

    assert_receive {[:db_connection, :checkout], _, %{connection_module: TestConnection} = ok}
    refute Map.has_key?(ok, :error)

    assert_receive {[:db_connection, :checkout], %{wait_time: wait_time},
                    %{
                      label: :my_pool,
                      connection_module: TestConnection,
                      error: %DBConnection.ConnectionError{reason: :queue_timeout},
                      timed_out: true
                    }}

    assert wait_time > 0
    assert_receive {[:db_connection, :checkin], _, _}
  end

  test "a checkout whose deadline passes while queued is a timed-out checkout with its error" do
    {:ok, agent} =
      A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}, :ok, {:ok, :state}])

    {:ok, pool} = P.start_link(agent: agent, parent: self(), queue_target: 1_000)
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

    assert_raise DBConnection.ConnectionError, ~r/deadline reached while in queue/, fn ->
      P.run(pool, fn _conn -> flunk("got connection") end, timeout: 10)
    end

    assert_receive {[:db_connection, :checkout], _,
                    %{
                      error: %DBConnection.ConnectionError{reason: :error, message: message},
                      timed_out: true
                    }}

    assert message =~ "deadline reached while in queue"
  end

  test "after_connect checks out with the pool's label and connection module" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])
    test = self()
    id = make_ref()

    :telemetry.attach(
      id,
      [:db_connection, :checkout],
      fn _event, _measurements, metadata, _ ->
        if metadata.label == :after_connect_pool, do: send(test, {:checkout, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, _pool} =
      P.start_link(
        agent: agent,
        parent: self(),
        label: :after_connect_pool,
        after_connect: fn _conn -> send(test, :after_connect) end
      )

    assert_receive :after_connect
    assert_receive {:checkout, %{connection_module: TestConnection}}
  end

  test "a checkout from a pool on an unreachable node fails with the pool's error" do
    pool = :erlang.binary_to_term(<<131, 88, 100, 0, 9, "a@nowhere", 0::32, 0::32, 0::32>>)
    attach(pool)

    assert {{:badnode, :a@nowhere}, _} =
             catch_exit(P.run(pool, fn _conn -> flunk("got connection") end))

    assert_receive {[:db_connection, :checkout], %{wait_time: _},
                    %{pool: ^pool, label: nil, connection_module: nil, error: _}}
  end

  test "a checkout from a pool that is not running is a checkout with its error" do
    {:ok, agent} = A.start_link([{:ok, :state}, :ok])
    {:ok, pool} = P.start_link(agent: agent, parent: self())
    attach(pool)
    Process.unlink(pool)
    ref = Process.monitor(pool)
    Process.exit(pool, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}

    catch_exit(P.run(pool, fn _conn -> flunk("got connection") end))

    assert_receive {[:db_connection, :checkout], %{wait_time: _},
                    %{pool: ^pool, label: nil, connection_module: nil, error: _, timed_out: false}}
  end

  test "a starting pool reports its pid, size and label, and the pid serves its connection metrics" do
    test = self()
    id = make_ref()
    label = {:pool_init_test, id}

    :telemetry.attach(
      id,
      [:db_connection, :pool, :init],
      fn _, measurements, metadata, _ ->
        if metadata.label == label, do: send(test, {:pool_init, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, agent} = A.start_link([{:ok, :state}])
    {:ok, _pool} = P.start_link(agent: agent, parent: self(), label: label, pool_size: 1)

    assert_receive {:pool_init, %{pool_size: 1},
                    %{pool: pool_pid, label: ^label, connection_module: TestConnection}}

    assert [%{source: {:pool, ^pool_pid}}] =
             DBConnection.ConnectionPool.get_connection_metrics(pool_pid)
  end
end
