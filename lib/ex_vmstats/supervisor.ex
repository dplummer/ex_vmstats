defmodule ExVmstats.Supervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    children = [
      worker(ExVmstats, [])
    ]

    supervise(children, strategy: :one_for_one)
  end
end
