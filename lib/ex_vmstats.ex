defmodule ExVmstats do
  use GenServer

  @timer_msg :interval_elapsed

  defmodule State do
    defstruct [
      :backend,
      :use_histogram,
      :interval,
      :sched_time,
      :prev_sched,
      :timer_ref,
      :namespace,
      :prev_io,
      :prev_gc
    ]

    def new(conf \\ []) do
      interval = kword_or_app(conf, :interval, 3000)
      %__MODULE__{
        interval: interval,
        namespace: kword_or_app(conf, :namespace, "vm_stats"),
        use_histogram: kword_or_app(conf, :use_histogram, false),
        sched_time: sched_time(kword_or_app(conf, :sched_time, false)),
        backend: backend(kword_or_app(conf, :backend, :ex_statsd)),
        prev_sched: prev_sched(),
        timer_ref: ExVmstats.start_timer(interval),
        prev_io: prev_io(),
        prev_gc: :erlang.statistics(:garbage_collection)
      }
    end

    defp prev_io do
      {{:input, input}, {:output, output}} = :erlang.statistics(:io)
      {input, output}
    end

    defp prev_sched do
      :erlang.statistics(:scheduler_wall_time)
      |> Enum.sort
    end

    defp sched_time(enabled) do
      case {sched_time_available?(), enabled} do
        {true, true} -> :enabled
        {true, _} -> :disabled
        {false, _} -> :unavailable
      end
    end

    defp backend(:ex_statsd), do: ExVmstats.Backends.ExStatsD
    defp backend(backend), do: backend

    defp kword_or_app(conf, key, default) do
      Application.get_env(:ex_vmstats, key, Keyword.get(conf, key, default))
    end

    defp sched_time_available? do
      try do
        :erlang.system_flag(:scheduler_wall_time, true)
      else
        _ -> true
      catch
        _ -> true
      rescue
        ArgumentError -> false
      end
    end
  end

  def start_link(conf \\ []) do
    GenServer.start_link(__MODULE__, conf)
  end

  def init(conf) do
    {:ok, State.new(conf)}
  end

  def handle_info({:timeout, _timer_ref, @timer_msg}, state) do
    %State{interval: interval, namespace: namespace, backend: backend} = state

    metric_name = fn (name) -> metric(namespace, name) end
    memory_metric_name = fn (name) -> memory_metric(namespace, name) end

    # Processes
    gauge_or_hist(state, :erlang.system_info(:process_count), metric_name.("proc_count"))
    gauge_or_hist(state, :erlang.system_info(:process_limit), metric_name.("proc_limit"))

    # Messages in queues
    total_messages =
      Enum.reduce Process.list, 0, fn pid, acc ->
        case Process.info(pid, :message_queue_len) do
          {:message_queue_len, count} -> count + acc
          _ -> acc
        end
      end

    gauge_or_hist(state, total_messages, metric_name.("messages_in_queues"))

    # Modules loaded
    gauge_or_hist(state, length(:code.all_loaded), metric_name.("modules"))

    # Queued up processes (lower is better)
    gauge_or_hist(state, :erlang.statistics(:run_queue), metric_name.("run_queue"))

    # Error logger backlog (lower is better)
    error_logger_backlog =
      Process.whereis(:error_logger)
      |> Process.info(:message_queue_len)
      |> elem(1)

    gauge_or_hist(state, error_logger_backlog, metric_name.("error_logger_queue_len"))

    # Memory usage. There are more options available, but not all were kept.
    # Memory usage is in bytes.
    mem = :erlang.memory

    for metric <- [:total, :processes_used, :atom_used, :binary, :ets] do
      gauge_or_hist(state, Keyword.get(mem, metric), memory_metric_name.(metric))
    end

    # Incremental values
    %State{prev_io: {old_input, old_output}, prev_gc: {old_gcs, old_words, _}} = state

    {{:input, input}, {:output, output}} = :erlang.statistics(:io)

    gc = {gcs, words, _} = :erlang.statistics(:garbage_collection)

    backend.counter(input - old_input, metric_name.("io.bytes_in"))
    backend.counter(output - old_output, metric_name.("io.bytes_out"))
    backend.counter(gcs - old_gcs, metric_name.("gc.count"))
    backend.counter(words - old_words, metric_name.("gc.words_reclaimed"))

    # Reductions across the VM, excluding current time slice, already incremental
    {_, reds} = :erlang.statistics(:reductions)

    backend.counter(reds, metric_name.("reductions"))

    #Scheduler wall time
    sched =
      case state.sched_time do
        :enabled ->
          new_sched = Enum.sort(:erlang.statistics(:scheduler_wall_time))

          for {sid, active, total} <- wall_time_diff(state.prev_sched, new_sched) do
            scheduler_metric_base = "#{namespace}.scheduler_wall_time.#{sid}"

            backend.timer(active, scheduler_metric_base <> ".active")
            backend.timer(total, scheduler_metric_base <> ".total")
          end

          new_sched
        _ ->
          nil
      end

    timer_ref = start_timer(interval)

    {:noreply, %{state | timer_ref: timer_ref, prev_sched: sched, prev_io: {input, output}, prev_gc: gc}}
  end

  def start_timer(interval) do
    :erlang.start_timer(interval, self(), @timer_msg)
  end

  defp metric(namespace, metric) do
    "#{namespace}.#{metric}"
  end

  defp memory_metric(namespace, metric) do
    "#{namespace}.memory.#{metric}"
  end

  defp gauge_or_hist(%State{use_histogram: true, backend: backend}, value, metric) do
    backend.histogram(value, metric)
  end
  defp gauge_or_hist(%State{backend: backend}, value, metric), do: backend.gauge(value, metric)

  defp wall_time_diff(prev_sched, new_sched) do
    for {{i, prev_active, prev_total}, {i, new_active, new_total}} <- Enum.zip(prev_sched, new_sched) do
      {i, new_active - prev_active, new_total - prev_total}
    end
  end
end
