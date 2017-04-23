defmodule BunnehMQ.WorkerPool.Worker do
  @moduledoc """
  Executes jobs (functions) submitted to a worker pool with BunnehMQ.WorkerPool.submit/1,
  BunnehMQ.WorkerPool.submit/2 or BunnehMQ.WorkerPool.submit_async/1.

  See BunnehMQ.WorkerPool for an overview.
  """
  @behaviour :gen_server2

  @type mfargs() :: {atom(), atom(), [any()]}

  @spec start_link(atom()) :: {:ok, pid()} | {:error, any()}
  @spec next_job_from(pid(), pid()) :: :ok
  @spec submit(pid(), (() -> any()) | mfargs(), :reuse | :single) :: any()
  @spec submit_async(pid(), fun (() -> any()) | mfargs()) :: :ok
  @spec run((() -> any()) | mfargs()) :: any()
  @spec set_maximum_since_use(pid(), non_neg_integer()) :: :ok

  @hibernate_after_min 1_000
  @desired_hibernate 10_000

  def start_link(pool_name), do: :gen_server2.start_link(__MODULE__, [pool_name], timeout: :infinity)

  def next_job_from(pid, cpid), do: :gen_server2.cast(pid, {:next_job_from, cpid})

  def submit(pid, fun, process_model),
    do: :gen_server2.call(pid, {:submit, fun, self(), process_model}, :infinity)

  def submit_async(pid, fun), do: :gen_server2.cast(pid, {:submit_async, fun})

  def set_maximum_since_use(pid, age), do: :gen_server2.cast(pid, {:set_maximum_since_use, age})

  def run({m, f, a}), do: apply(m, f, a)
  def run(fun), do: fun.()

  def run(fun, :reuse), do: run(fun)
  def run(fun, :single) do
    self = self()
    ref = make_ref()

    spawn_link(fn ->
      Process.put(BunnehMQ.WorkerPool.Worker, true)
      send(self, {ref, run(fun)})
      Process.unlink(self)
    end)

    receive do
      {^ref, res} -> res
    end
  end

  def init([pool_name]) do
    :ok = BunnehMQ.FileHandleCache.register_callback(__MODULE__, :set_maximum_since_use, [self()])
    :ok = BunnehMQ.WorkerPool.ready(pool_name, self())
    Process.put(BunnehMQ.WorkerPool.Worker, true)
    Process.put(:worker_pool_name, pool_name)

    {:ok, :undefined, :hibernate, {:backoff, @hibernate_after_min, @hibernate_after_min, @desired_hibernate}}
  end

  def prioritise_cast({:set_maximum_since_use, _age}, _len, _state), do: 8
  def prioritise_cast({:next_job_from, _cpid}, _len, _state), do: 7
  def prioritise_cast(_msg, _len, _state), do: 0

  def handle_call({:submit, fun, cpid, process_model}, from, :undefined),
    do: {:noreply, {:job, cpid, from, fun, process_model}, :hibernate}
  def handle_call({:submit, fun, cpid, process_model}, from, {:from, cpid, mref}) do
    Process.demonitor(mref)
    :gen_server2.reply(from, run(fun, process_model))
    :ok = BunnehMQ.WorkerPool.idle(Process.get(:worker_pool_name), self())
    {:noreply, :undefined, :hibernate}
  end

  def handle_call(msg, _from, state), do: {:stop, {:unexpected_call, msg}, state}

  def handle_cast({:next_job_from, cpid}, :undefined),
    do: {:noreply, {:from, cpid, Process.monitor(cpid)}, :hibernate}
  def handle_cast({:next_job_from, cpid}, {:job, cpid, from, fun, process_model}) do
    :gen_server2.reply(from, run(fun, process_model))
    :ok = BunnehMQ.WorkerPool.idle(Process.get(:worker_pool_name), self())
    {:noreply, :undefined, :hibernate}
  end

  def handle_cast({:submit_async, fun}, :undefined) do
    run(fun)
    :ok = BunnehMQ.WorkerPool.idle(Process.get(:worker_pool_name), self())
    {:noreply, :undefined, :hibernate}
  end

  def handle_cast({:set_maximum_since_use, age}, state) do
    :ok = BunnehMQ.FileHandleCache.set_maximum_since_use(age)
    {:noreply, state, :hibernate}
  end

  def handle_cast(msg, state), do: {:stop, {:unexpected_cast, msg}, state}

  def handle_info({:DOWN, mref, :process, cpid, _reason}, {:from, cpid, mref}) do
    :ok = BunnehMQ.WorkerPool.idle(Process.get(:worker_pool_name), self())
    {:noreply, :undefined, :hibernate}
  end
  def handle_info({:DOWN, _mref, :process, _pid, _reason}, state),
    do: {:noreply, state, :hibernate}

  def handle_info({:timeout, key, fun}, state) do
    clear_timeout(key)
    fun.()
    {:noreply, state, :hibernate}
  end

  def handle_info(msg, state), do: {:stop, {:unexpected_info, msg}, state}

  def code_change(_old_vsn, state, _extra), do: {:ok, state}

  def terminate(_reason, state), do: state

  @spec set_timeout(integer(), (() -> any())) :: reference()
  def set_timeout(time, fun), do: set_timeout(make_ref(), time, fun)

  @spec set_timeout(key :: any(), integer(), (() -> any())) :: any()
  def set_timeout(key, time, fun), do: set_timeout(key, time, fun, get_timeouts())

  defp clear_timeout(key) do
    new_timeouts = cancel_timeout(key, get_timeouts())
    Process.put(:timeouts, new_timeouts)
    :ok
  end

  defp get_timeouts do
    case Process.get(:timeouts) do
      :undefined -> :dict.new()
      dict -> dict
    end
  end

  defp set_timeout(key, time, fun, timeouts) do
    cancel_timeout(key, timeouts)
    {:ok, tref} = :timer.send_after(time, {:timeout, key, fun})
    new_timeouts = :dict.store(key, tref, timeouts)
    Process.put(:timeouts, new_timeouts)
    {:ok, key}
  end

  defp cancel_timeout(key, timeouts) do
    case :dict.find(key, timeouts) do
      {:ok, tref} ->
        :timer.cancel(tref)
        receive do
          {:timeout, ^key, _} -> :ok
        after
          0 -> :ok
        end
        :dict.erase(key, timeouts)
      :error ->
        timeouts
    end
  end
end
