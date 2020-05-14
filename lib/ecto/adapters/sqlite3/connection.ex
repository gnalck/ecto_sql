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

    @impl Ecto.Adapters.SQL.Connection
    def insert(prefix, table, header, rows, _on_conflict, _returning) do
      fields = intersperse_map(header, ?,, &quote_name/1)
      ["INSERT INTO", quote_table(prefix, table), " (", fields, ") VALUES ", insert_all(rows)]
    end

    # def insert(_prefix, _table, _header, _rows, _on_conflict, _returning) do
    #   IO.puts(inspect(_returning))
    #   raise ArgumentError, ":returning is not supported in insert/insert_all by SQLite3"
    # end

    @impl Ecto.Adapters.SQL.Connection
    @spec execute_ddl(command :: Ecto.Adapter.Migration.command()) :: String.t() | [iodata]
    def execute_ddl

    defp insert_all(rows) do
      intersperse_map(rows, ?,, fn row ->
        [?(, intersperse_map(row, ?,, &insert_all_value/1), ?)]
      end)
    end

    defp insert_all_value(nil),
      do: raise(ArgumentError, "SQLite3 requires each specified column to be provided a value)")

    # defp insert_all_value({%Ecto.Query{} = query, _params_counter}), do: [?(, all(query), ?)]
    defp insert_all_value(_), do: '?'

    defp intersperse_map(list, separator, mapper, acc \\ [])
    defp intersperse_map([], _separator, _mapper, acc), do: acc
    defp intersperse_map([elem], _separator, mapper, acc), do: [acc | mapper.(elem)]

    defp intersperse_map([elem | rest], separator, mapper, acc),
      do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

    defp quote_name(name)
    defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))

    defp quote_name(name) do
      if String.contains?(name, "`") do
        raise ArgumentError, "bad field name #{inspect(name)}"
      end

      [?`, name, ?`]
    end

    defp quote_table(nil, name), do: quote_table(name)
    defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

    defp quote_table(name) when is_atom(name),
      do: quote_table(Atom.to_string(name))

    defp quote_table(name) do
      if String.contains?(name, "`") do
        raise ArgumentError, "bad table name #{inspect(name)}"
      end

      [?`, name, ?`]
    end
  end
end
