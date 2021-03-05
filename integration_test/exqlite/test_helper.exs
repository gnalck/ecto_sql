Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :id)
Application.put_env(:ecto, :async_integration_tests, false)


Code.require_file "../support/repo.exs", __DIR__

alias Ecto.Integration.TestRepo

# should be :ecto_sql ?
Application.put_env(:ecto_sql, TestRepo,
  adapter: Ecto.Adapters.Exqlite,
  database: "/tmp/exqlite_sandbox_test.db",
  journal_mode: :wal,
  cache_size: -64000,
  temp_store: :memory,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,
  show_sensitive_data_on_connection_error: true
)

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto_sql, adapter: Ecto.Adapters.Exqlite

  def create_prefix(prefix) do
    "attach database #{prefix}.db as #{prefix}"
  end

  def drop_prefix(prefix) do
    "detach database #{prefix}.db"
  end

  def uuid do
    Ecto.UUID
  end
end

alias Ecto.Integration.PoolRepo

Application.put_env(:ecto_sql, PoolRepo,
  database: "/tmp/exqlite_sandbox_test.db",
  journal_mode: :wal,
  cache_size: -64000,
  temp_store: :memory,
  pool_size: 1,
  show_sensitive_data_on_connection_error: true
)

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :ecto_sql, adapter: Ecto.Adapters.Exqlite
end

ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file "#{ecto}/integration_test/support/schemas.exs", __DIR__
Code.require_file "../support/migration.exs", __DIR__

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(TestRepo)
  end
end

{:ok, _} = Ecto.Adapters.Exqlite.ensure_all_started(TestRepo.config(), :temporary)

# Load up the repository, start it, and run migrations
_   = Ecto.Adapters.Exqlite.storage_down(TestRepo.config())
:ok = Ecto.Adapters.Exqlite.storage_up(TestRepo.config())

{:ok, _pid} = TestRepo.start_link()
{:ok, _pid} = PoolRepo.start_link()

ExUnit.configure(
  exclude: [
    :array_type,
  ]
)

:ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false)
Ecto.Adapters.SQL.Sandbox.mode(TestRepo, :manual)
Process.flag(:trap_exit, true)

ExUnit.start()
