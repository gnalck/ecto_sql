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
      XQLite3.prepare_execute(conn, name, sql, params, opts)
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
      ["INSERT INTO ", quote_table(prefix, table), " (", fields, ") VALUES ", insert_all(rows)]
    end

    # def insert(_prefix, _table, _header, _rows, _on_conflict, _returning) do
    #   IO.puts(inspect(_returning))
    #   raise ArgumentError, ":returning is not supported in insert/insert_all by SQLite3"
    # end

    alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr, WithExpr, Tagged}

    @impl true
    def all(query, as_prefix \\ []) do
      IO.inspect(query)
      sources = create_names(query, as_prefix)

      cte = [] #cte(query, sources)
      from = from(query, sources)
      IO.inspect(from)
      select = select(query, sources)
      IO.inspect(select)
      join = [] #join(query, sources)
      where = [] #where(query, sources)
      group_by = [] #group_by(query, sources)
      having = [] #having(query, sources)
      window = [] #window(query, sources)
      combinations = [] #combinations(query)
      order_by = [] #order_by(query, sources)
      limit = [] #limit(query, sources)
      offset = [] #offset(query, sources)
      lock = [] #lock(query, sources)

      [cte, select, from, join, where, group_by, having, window, combinations, order_by, limit, offset | lock]
    end

    # Migrations

    alias Ecto.Migration.{Table, Index, Reference, Constraint}

    @impl true
    def ddl_logs(_), do: []

    @impl true
    @spec execute_ddl(command :: Ecto.Adapter.Migration.command()) :: String.t() | [iodata]
    def execute_ddl({command, %Table{} = table, columns})
        when command in [:create, :create_if_not_exists] do
      table_name = quote_table(table.prefix, table.name)

      #IO.inspect(columns)

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

    def execute_ddl({command, %Index{} = index}) do
      fields = intersperse_map(index.columns, ", ", &index_expr/1)

      [
        [
          "CREATE ",
          if_do(index.unique, "UNIQUE "),
          "INDEX ",
          if_do(command == :create_if_not_exists, "IF NOT EXISTS "),
          quote_name(index.name),
          " ON ",
          quote_table(index.prefix, index.table),
          ?\s, ?(, fields, ?),
          if_do(index.where, [" WHERE ", to_string(index.where)])
        ]
      ]
    end

    def execute_ddl(string) when is_binary(string), do: [string]

    @impl true
    def table_exists_query(table) do
      {"SELECT true FROM sqlite_master where type = 'table' and name = $1", [table]}
    end

    # impl

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

    defp escape_string(value) when is_binary(value) do
      value |> :binary.replace("'", "''", [:global])
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

    defp column_options(type, opts) do
      default = Keyword.fetch(opts, :default)
      null = Keyword.get(opts, :null)

      # todo: default_expr
      [null_expr(null)]
    end

    defp null_expr(false), do: " NOT NULL"
    defp null_expr(true), do: " NULL"
    defp null_expr(_), do: []

    defp error!(message), do: error!(nil, message)

    defp error!(nil, message) do
      raise ArgumentError, message
    end

    defp error!(query, message) do
      raise Ecto.QueryError, query: query, message: message
    end

    defp create_names(%{sources: sources}, as_prefix) do
      create_names(sources, 0, tuple_size(sources), as_prefix) |> List.to_tuple()
    end

    defp create_names(sources, pos, limit, as_prefix) when pos < limit do
      [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
    end

    defp create_names(_sources, pos, pos, _as_prefix) do
      []
    end

    defp create_name(sources, pos, as_prefix) do
      case elem(sources, pos) do
        {:fragment, _, _} ->
          {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

        {table, schema, prefix} ->
          name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
          {quote_table(prefix, table), name, schema}

        %Ecto.SubQuery{} ->
          {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
      end
    end

    defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
      first
    end

    defp create_alias(_) do
      ?t
    end

    defp from(%{from: %{hints: [_ | _]}} = query, _sources) do
      error!(query, "table hints are not supported by SQLite3")
    end

    defp from(%{from: %{source: source}} = query, sources) do
      {from, name} = get_source(query, sources, 0, source)
      [" FROM ", from, " AS " | name]
    end

    defp get_source(query, sources, ix, source) do
      {expr, name, _schema} = elem(sources, ix)
      {expr || expr(source, sources, query), name}
    end

    defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
      [
        "SELECT ",
        distinct(distinct, sources, query),
        select(fields, sources, query)
      ]
    end

    defp select([], _sources, _query), do: "TRUE"

    defp select(fields, sources, query) do
      intersperse_map(fields, ", ", fn
        {:&, _, [idx]} ->
          case elem(sources, idx) do
            {source, _, nil} ->
              error!(query, "MySQL does not support selecting all fields from #{source} without a schema. " <>
                            "Please specify a schema or specify exactly which fields you want to select")
            {_, source, _} ->
              source
          end
        {key, value} ->
          [expr(value, sources, query), " AS ", quote_name(key)]
        value ->
          expr(value, sources, query)
      end)
    end

    defp distinct(nil, _sources, _query), do: []
    defp distinct(%QueryExpr{expr: true}, _sources, _query),  do: "DISTINCT "
    defp distinct(%QueryExpr{expr: false}, _sources, _query), do: []

    defp distinct(%QueryExpr{expr: exprs}, _sources, query) when is_list(exprs) do
      error!(query, "DISTINCT with multiple columns is not supported by SQLite3")
    end

    defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
      quote_qualified_name(field, sources, idx)
    end

    defp expr(%Tagged{value: other, type: type}, sources, query) do
      ["CAST(", expr(other, sources, query), " AS ", ecto_to_db(type, query), ?)]
    end

    defp expr(nil, _sources, _query),   do: "NULL"
    defp expr(true, _sources, _query),  do: "TRUE"
    defp expr(false, _sources, _query), do: "FALSE"

    defp expr(literal, _sources, _query) when is_binary(literal) do
      [?', escape_string(literal), ?']
    end

    defp expr(literal, _sources, _query) when is_integer(literal) do
      Integer.to_string(literal)
    end

    defp expr(literal, _sources, _query) when is_float(literal) do
      Float.to_string(literal)
    end

    defp index_expr(literal) when is_binary(literal), do: literal
    defp index_expr(literal), do: quote_name(literal)

    defp quote_qualified_name(name, sources, ix) do
      {_, source, _} = elem(sources, ix)
      [source, ?. | quote_name(name)]
    end

    defp column_type(atom, query), do: ecto_to_db(atom, query)

    defp ecto_to_db({:map, _}, _query), do: "text"
    defp ecto_to_db(:naive_datetime, _query), do: "datetime"
    defp ecto_to_db(:binary, _query), do: "blob"
    defp ecto_to_db(type, _query), do: Atom.to_string(type)

  end
end
