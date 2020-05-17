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


end
