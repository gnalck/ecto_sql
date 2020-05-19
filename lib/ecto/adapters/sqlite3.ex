defmodule Ecto.Adapters.SQLite3 do
  @moduledoc """
  Adapter module for SQLite3.

  It uses `XQLite3` for communicating to the database.
  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :xqlite3,
    migration_lock: nil

  @behaviour Ecto.Adapter.Storage
  # @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter.Storage
  @spec storage_up(options :: Keyword.t()) :: :ok | {:error, :already_up} | {:error, term}
  def storage_up(opts) do
    path = Keyword.fetch!(opts, :path) || raise ":path is nil in repository configuration"

    if File.exists?(path) do
      {:error, :already_up}
    else
      File.touch(path)
    end
  end

  @impl Ecto.Adapter.Storage
  @spec storage_down(options :: Keyword.t()) :: :ok | {:error, :already_down} | {:error, term}
  def storage_down(opts) do
    path = Keyword.fetch!(opts, :path) || raise ":path is nil in repository configuration"

    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> {:error, :already_down}
      {:error, :enotdir} -> {:error, :already_down}
      {:error, err} -> {:error, Exception.message(err)}
    end
  end

  @impl Ecto.Adapter.Storage
  @spec storage_status(options :: Keyword.t()) :: :up | :down | {:error, term()}
  def storage_status(opts) do
    path = Keyword.fetch!(opts, :path) || raise ":path is nil in repository configuration"

    case File.exists?(path) do
      true -> :up
      false -> :down
    end
  end

  @impl true
  def supports_ddl_transaction? do
    false
  end

  @impl true
  def insert(adapter_meta, schema_meta, params, on_conflict, returning, opts) do
    %{source: source, prefix: prefix} = schema_meta
    {_, query_params, _} = on_conflict

    key = primary_key!(schema_meta, returning)
    {fields, values} = :lists.unzip(params)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, [])
    opts = [{:cache_statement, "ecto_insert_#{source}"} | opts]

    case Ecto.Adapters.SQL.query(adapter_meta, sql, values ++ query_params, opts) do
      {:ok, %{num_rows: 1, last_insert_id: last_insert_id}} ->
        {:ok, last_insert_id(key, last_insert_id)}

      {:error, err} ->
        case @conn.to_constraints(err, source: source) do
          []          -> raise err
          constraints -> {:invalid, constraints}
        end
    end
  end

  defp primary_key!(%{autogenerate_id: {_, key, _type}}, [key]), do: key
  defp primary_key!(_, []), do: nil
  defp primary_key!(%{schema: schema}, returning) do
    raise ArgumentError, "SQLite3 does not support :read_after_writes in schemas for non-primary keys. " <>
                         "The following fields in #{inspect schema} are tagged as such: #{inspect returning}"
  end

  defp last_insert_id(nil, _last_insert_id), do: []
  defp last_insert_id(_key, 0), do: []
  defp last_insert_id(key, last_insert_id), do: [{key, last_insert_id}]


end
