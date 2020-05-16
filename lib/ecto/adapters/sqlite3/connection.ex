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

    @impl true
    def prepare_execute(conn, name, sql, params, opts) do
      SQLite3.prepare_execute(conn, name, sql, params, opts)
    end

    @impl true
    def execute(conn, query, params, opts) do
      case SQLite3.execute(conn, query, params, opts) do
        {:ok, _, result} -> {:ok, result}
        {:error, _} = error -> error
      end
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

    alias Ecto.Migration.{Table, Index, Reference, Constraint}

    @impl true
    def ddl_logs(_), do: []

    @impl true
    @spec execute_ddl(command :: Ecto.Adapter.Migration.command()) :: String.t() | [iodata]
    def execute_ddl({command, %Table{} = table, columns})
        when command in [:create, :create_if_not_exists] do
      table_name = quote_table(table.prefix, table.name)

      query = [[
        "CREATE TABLE ",
        if_do(command == :create_if_not_exists, "IF NOT EXISTS "),
        table_name,
        " (",
        column_definitions(table, columns),
        # pk_definitions(columns, ", "),
        ?)
      ]]
    end

    def execute_ddl({command, %Table{} = table, columns})
        when command in [:drop, :drop_if_exists] do
      [
        [
          "DROP TABLE ",
          if_do(command == :drop_if_exists, "IF EXISTS "),
          quote_table(table.prefix, table.name)
        ]
      ]
    end

    def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
      [
        [
          "ALTER TABLE ",
          quote_table(current_table.prefix, current_table.name),
          " RENAME TO ",
          quote_table(nil, new_table.name)
        ]
      ]
    end

    def execute_ddl({:rename, %Table{} = table, current_column, new_column}) do
      [
        [
          "ALTER TABLE ",
          quote_table(table.prefix, table.name),
          " RENAME ",
          quote_name(current_column),
          " TO ",
          quote_name(new_column)
        ]
      ]
    end

    def execute_ddl(string) when is_binary(string), do: [string]

    @impl true
    def table_exists_query(table) do
      {"SELECT true FROM sqlite_master where type = 'table' and name = $1", [table]}
    end

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

    defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))
    defp quote_name(name), do: name

    defp quote_table(nil, name), do: quote_table(name)
    defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]
    defp quote_table(name) when is_atom(name), do: quote_table(Atom.to_string(name))
    defp quote_table(name), do: name

    defp if_do(condition, value) do
      if condition, do: value, else: []
    end

    defp column_definitions(table, columns) do
      intersperse_map(columns, ", ", &column_definition(table, &1))
    end

    defp column_definition(_table, {:add, name, %Reference{} = ref, opts}) do
      [
        quote_name(name),
        ?\s,
        column_type(ref.type, opts),
        column_options(ref.type, opts)
        # reference_expr(ref, table, name)
      ]
    end

    defp column_definition(_table, {:add, name, type, opts}) do
      [quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
    end

    # todo: handle datetimes better
    defp column_type(atom, _query), do: Atom.to_string(atom)

    defp column_options(type, opts) do
      default = Keyword.fetch(opts, :default)
      null = Keyword.get(opts, :null)

      # todo: default_expr
      [null_expr(null)]
    end

    defp null_expr(false), do: " NOT NULL"
    defp null_expr(true), do: " NULL"
    defp null_expr(_), do: []
  end
end
