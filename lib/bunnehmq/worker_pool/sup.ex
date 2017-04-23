defmodule BunnehMQ.WorkerPool.Sup do
  use Supervisor

  @spec start_link() :: :rabbit_types.ok_pid_or_error()
  def start_link, do: start_link(:erlang.system_info(:schedulers))

  @spec start_link(non_neg_integer()) :: :rabbit_types.ok_pid_or_error()
  def start_link(wcount), do: start_link(wcount, BunnehMQ.WorkerPool.default_pool())

  @spec start_link(non_neg_integer(), atom()) :: :rabbit_types.ok_pid_or_error()
  def start_link(wcount, pool_name) do
    sup_name = Module.concat(pool_name, Sup)
    Supervisor.start_link(__MODULE__, [wcount, pool_name], name: sup_name)
  end

  def init([wcount, pool_name]) do
    import Supervisor.Spec
    # we want to survive up to 1K of worker restarts per second,
    # e.g. when a large worker pool used for network connections
    # encounters a network failure. This is the case in the LDAP authentication
    # backend plugin.

    # worker(module, [term], [restart: restart, shutdown: shutdown, id: term, function: atom, modules: modules]) :: spec
    # spec() :: {child_id, start_fun :: {module, atom, [term]}, restart, shutdown, worker, modules}
    # {worker_pool, {worker_pool, start_link, [PoolName]}, transient, 16#ffffffff, worker, [worker_pool]}
    pool = worker(pool_name, [pool_name], restart: :transient, shutdown: 0xffffffff)

    workers = Enum.map(1..wcount, fn wnum ->
      worker(BunnehMQ.WorkerPool.Worker, [pool_name], restart: :transient, shutdown: 0xffffffff, id: wnum)
    end)

    children = [pool | workers]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
