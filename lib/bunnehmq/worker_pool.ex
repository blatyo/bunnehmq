defmodule BunnehMQ.WorkerPool do
  @moduledoc """
  Generic worker pool manager.

  Submitted jobs are functions. They can be executed synchronously
  (using worker_pool:submit/1, worker_pool:submit/2) or asynchronously
  (using worker_pool:submit_async/1).

  We typically use the worker pool if we want to limit the maximum
  parallelism of some job. We are not trying to dodge the cost of
  creating Erlang processes.

  Supports nested submission of jobs and two execution modes:
  'single' and 'reuse'. Jobs executed in 'single' mode are invoked in
  a one-off process. Those executed in 'reuse' mode are invoked in a
  worker process out of the pool. Nested jobs are always executed
  immediately in current worker process.

  'single' mode is offered to work around a bug in Mnesia: after
  network partitions reply messages for prior failed requests can be
  sent to Mnesia clients - a reused worker pool process can crash on
  receiving one.

  Caller submissions are enqueued internally. When the next worker
  process is available, it communicates it to the pool and is
  assigned a job to execute. If job execution fails with an error, no
  response is returned to the caller.

  Worker processes prioritise certain command-and-control messages
  from the pool.

  Future improvement points: job prioritisation.
  """
  @behaviour :gen_server2

  @type mfargs() :: {atom(), atom(), [any()]}

  @spec start_link(atom()) :: {:ok, pid()} | {:error, any()}
  @spec submit((() -> any()) | mfargs()) :: any()
  @spec submit((() -> any()) | mfargs(), :reuse | :single) :: any()
  @spec submit(atom(), (() -> any()) | mfargs(), :reuse | :single) :: any()
  @spec submit_async((() -> any()) | mfargs()) :: :ok
  @spec ready(atom(), pid()) :: :ok
  @spec idle(atom(), pid()) :: :ok
  @spec default_pool() :: atom()

  @default_pool __MODULE__
  @hibernate_after_min 1_000
  @desired_hibernate 10_000

  defmodule State do
    defstruct [:available, :pending]
  end

  def start_link(name) do
    :gen_server2.start_link({:local, name}, __MODULE__, [], [timeout: :infinity])
  end

  def submit(fun), do: submit(@default_pool, fun, :reuse)
  def submit(fun, process_model), do: submit(@default_pool, fun, process_model)
  def submit(server, fun, process_model) do
    case Process.get(__MODULE__.Worker) do
      true -> __MODULE__.Worker.run(fun)
      _ ->
        pid = :gen_server2.call(server, {:next_free, self()}, :infinity)
        __MODULE__.Worker.submit(pid, fun, process_model)
    end
  end

  def submit_async(fun), do: submit_async(@default_pool, fun)
  def submit_async(server, fun), do: :gen_server2.cast(server, {:run_async, fun})

  def ready(server, wpid), do: :gen_server2.cast(server, {:ready, wpid})

  def idle(server, wpid), do: :gen_server2.cast(server, {:idle, wpid})

  def default_pool, do: @default_pool

  def init([]) do
    {:ok,
      %State{pending: :queue.new(), available: :ordsets.new()}, :hibernate,
      {:backoff, @hibernate_after_min, @hibernate_after_min, @desired_hibernate}
    }
  end

  def handle_call({:next_free, cpid}, from, %State{available: [], pending: pending} = state) do
    {:noreply, %{state | pending: :queue.in({:next_free, from, cpid}, pending)}, :hibernate}
  end
  def handle_call({:next_free, cpid}, _from, %State{available: [wpid | available]} = state) do
    __MODULE__.Worker.next_job_from(wpid, cpid)
    {:reply, wpid, %{state | available: available}, :hibernate}
  end

  def handle_call(msg, _from, state), do: {:stop, {:unexpected_call, msg}, state}

  def handle_cast({:ready, wpid}, state) do
    Process.monitor(wpid)
    handle_cast({:idle, wpid}, state)
  end

  def handle_cast({:idle, wpid}, %State{available: available, pending: pending} = state) do
    state = case :queue.out(pending) do
      {:empty, _pending} ->
        %{state | available: :ordsets.add_element(wpid, available)}
      {{:value, {:next_free, from, cpid}}, pending} ->
        __MODULE__.Worker.next_job_from(wpid, cpid)
        :gen_server2.reply(from, wpid)
        %{state | pending: pending}
      {{:value, {:run_async, fun}}, pending} ->
        __MODULE__.Worker.submit_async(wpid, fun)
        %{state | pending: pending}
    end

    {:noreply, state, :hibernate}
  end

  def handle_cast({:run_async, fun}, %State{available: [], pending: pending} = state) do
    {:noreply, %{state | pending: :queue.in({:run_async, fun}, pending)}, :hibernate}
  end
  def handle_cast({:run_async, fun}, %State{available: [wpid | available]} = state) do
    __MODULE__.Worker.submit_async(wpid, fun)
    {:noreply, %{state | available: available}, :hibernate}
  end

  def handle_cast(msg, state), do: {:stop, {:unexpected_cast, msg}, state}

  def handle_info({:DOWN, _mref, :process, wpid, _reason}, %State{available: available} = state)  do
    {:noreply, %{state | available: :ordsets.del_element(wpid, available)}, :hibernate}
  end

  def handle_info(msg, state), do: {:stop, {:unexpected_info, msg}, state}

  def code_change(_old_vsn, state, _extra), do: {:ok, state}

  def terminate(_reason, state), do: state
end
