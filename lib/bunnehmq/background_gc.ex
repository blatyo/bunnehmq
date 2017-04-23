defmodule BunnehMQ.BackgroundGC do
  @behaviour :gen_server2

  @max_interval 240000
  @max_ratio 0.01

  defmodule State do
    defstruct [:last_interval]
  end

  def start_link do
    :gen_server2.start_link({:local, __MODULE__}, __MODULE__, [], [{:timeout, :infinity}])
  end

  def run do
    :gen_server2.cast(__MODULE__, :run)
  end

  def init([]) do
    {:ok, ideal_interval} = Application.get_env(:rabbit, :background_gc_target_interval)
    {:ok, interval_gc(%State{last_interval: ideal_interval})}
  end

  def handle_call(msg, _from, state) do
    {:stop, {:unexpected_call, msg}, {:unexpected_call, msg}, state}
  end

  def handle_cast(:run, state) do
    gc()
    {:noreply, state}
  end
  def handle_cast(msg, state), do: {:stop, {:unexpected_cast, msg}, state}

  def handle_info(:run, state), do: {:noreply, interval_gc(state)}
  def handle_info(msg, state), do: {:stop, {:unexpected_info, msg}, state}

  def code_change(_old_vsn, state, _extra), do: {:ok, state}

  def terminate(_reason, state), do: state

  def interval_gc(%State{last_interval: last_interval} = state) do
    {:ok, ideal_interval} = Application.get_env(:rabbit, :background_gc_target_interval)
    {:ok, interval} = :rabbit_misc.interval_operation(
      {__MODULE__, :gc, []},
      @max_ratio, @max_interval, ideal_interval, last_interval
    )

    :erlang.send_after(interval, self(), :run)
    %{state | last_interval: interval}
  end

  def gc do
    enabled = :rabbit_misc.get_env(:rabbit, :background_gc_enabled, true)

    case enabled do
      true ->
        for p <- :erlang.processes(),
            {:status, :waiting} == :erlang.process_info(p, :status) do
          :erlang.garbage_collect(p)
        end
        :erlang.garbage_collect()
      false ->
        :ok
    end
  end
end
