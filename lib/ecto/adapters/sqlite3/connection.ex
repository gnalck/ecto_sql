if Code.ensure_loaded(XQLite3) do
  defmodule Ecto.Adapters.SQLite3.Connection do
    @behaviour Ecto.Adapters.SQL.Connection

    @impl Ecto.Adapters.SQL.Connection
    def child_spec(opts) do
      XQLite3.child_spec(opts)
    end

    @impl Ecto.Adapters.SQL.Connection
    def query(conn, sql, params, opts) do
      XQLite3.query(conn, sql, params, opts)
    end
  end
end
