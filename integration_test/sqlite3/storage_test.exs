Code.require_file("../support/file_helpers.exs", __DIR__)

defmodule Ecto.Integration.StorageTest do
  use ExUnit.Case

  import Support.FileHelpers

  setup do
    tmp_file = Path.join(tmp_path(), "test.db")
    on_exit(fn -> File.rm(tmp_file) end)
    {:ok, [params: [path: tmp_file]]}
  end

  test "storage up (twice in a row)", context do
    assert :ok == Ecto.Adapters.SQLite3.storage_up(context[:params])
    assert {:error, :already_up} == Ecto.Adapters.SQLite3.storage_up(context[:params])
  end

  test "storage down (twice in a row)", context do
    assert :ok == Ecto.Adapters.SQLite3.storage_up(context[:params])
    assert :ok == Ecto.Adapters.SQLite3.storage_down(context[:params])
    assert {:error, :already_down} == Ecto.Adapters.SQLite3.storage_down(context[:params])
  end

  test "storage status is up when database is created", context do
    assert :ok == Ecto.Adapters.SQLite3.storage_up(context[:params])
    assert :up == Ecto.Adapters.SQLite3.storage_status(context[:params])
  end

  test "storage status id down when database is not created", context do
    assert :down == Ecto.Adapters.SQLite3.storage_status(context[:params])
  end
end
