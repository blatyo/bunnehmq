defmodule BunnehMQ.BootStep do
  def run_boot_steps do
    run_boot_steps(loaded_applications())
  end

  def run_boot_steps(apps) do
    for {_, _, attrs} <- find_steps(apps) do
      :ok = run_step(attrs, :mfa)
    end
    :ok
  end

  def run_cleanup_steps(apps) do
    for {_, _, attrs} <- find_steps(apps), do: run_step(attrs, :cleanup)
    :ok
  end

  def loaded_applications do
    for {app, _, _} <- Application.loaded_applications(), do: app
  end

  def find_steps do
    find_steps(loaded_applications())
  end

  def find_steps(apps) do
    all = sort_boot_steps(:rabbit_misc.all_module_attributes(:bunneh_boot_step))

    for {app, _, _} = step <- all, Enum.member?(apps, app), do: step
  end

  defp run_step(attributes, attribute_name) do
    matched_mfas = for {key, mfa} <- attributes, key == attribute_name, do: mfa

    case matched_mfas do
      [] -> :ok
      mfas ->
        for {m, f, a} <- mfas do
          case apply(m, f, a) do
            :ok -> :ok
            {:error, reason} ->
              exit({:error, reason})
          end
        end
    end
  end

  defp vertices({app_name, _module, steps}) do
    for {step_name, attrs} <- steps, do: {step_name, {app_name, step_name, attrs}}
  end

  defp edges({_app_name, _module, steps}) do
    ensure_list = fn
      l when is_list(l) -> l
      t -> [t]
    end

    for {step_name, attrs} <- steps,
        {key, other_step_or_steps} <- attrs,
        other_step <- ensure_list.(other_step_or_steps),
        key == :requires or key == :enables do
      case key do
        :requires -> {step_name, other_step}
        :enables -> {other_step, step_name}
      end
    end
  end

  defp sort_boot_steps(unsorted_steps) do
    case :rabbit_misc.build_acyclic_graph(&vertices/1, &edges/1, unsorted_steps) do
      {:ok, g} ->
        steps = for step_name <- :digraph_utils.topsort(g) do
          {^step_name, step} = :digraph.vertex(g, step_name)

          step
        end
        sorted_steps = Enum.reverse(steps)
        :digraph.delete(g)

        fns =
          for {_app, step_name, attributes} <- sorted_steps,
              {:mfa, {m, f, a}} <- attributes,
              Code.ensure_loaded(m) != {:module, m} or not function_exported?(m, f, length(a)) do
            {step_name, {m, f, a}}
          end

        case fns do
          [] -> sorted_steps
          missing_fns -> exit({:boot_functions_not_exported, missing_fns})
        end
      {:error, {:vertex, :duplicate, step_name}} ->
        exit({:duplicate_boot_step, step_name})
      {:error, {:edge, reason, from, to}} ->
        exit({:invalid_boot_step_dependency, from, to, reason})
    end
  end
end
