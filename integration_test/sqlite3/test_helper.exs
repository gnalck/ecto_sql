Logger.configure(level: :info)

# configure
Application.put_env(:ecto, :primary_key_type, :id)
Application.put_env(:ecto, :async_integration_tests, false)
# TODO? lock_for_update

# add some extra telemetry
Code.require_file("../support/repo.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(
  :ecto_sql,
  TestRepo,
  pool: Ecto.Adapters.SQL.Sandbox,
  path: "test.db"
)

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto_sql, adapter: Ecto.Adapters.SQLite3

  def create_prefix(prefix) do
    raise "todo"
  end

  def drop_prefix(prefix) do
    raise "todo"
  end

  def uuid do
    Ecto.UUID
  end
end

# Load support files
ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)
Code.require_file("../support/migration.exs", __DIR__)

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(TestRepo)
  end
end

{:ok, _} = Ecto.Adapters.SQLite3.ensure_all_started(TestRepo.config(), :temporary)

_ = Ecto.Adapters.SQLite3.storage_down(TestRepo.config())
:ok = Ecto.Adapters.SQLite3.storage_up(TestRepo.config())

{:ok, _pid} = TestRepo.start_link()

ExUnit.configure(
  exclude: [
    :array_type,
    :returning
  ]
)

:ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: :debug)
Ecto.Adapters.SQL.Sandbox.mode(TestRepo, :manual)
Process.flag(:trap_exit, true)

ExUnit.start()
