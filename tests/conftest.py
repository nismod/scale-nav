import ibis as ib


def table_from_df(conn, name, df, overwrite=True):
    """Create a DuckDB table from a pandas DataFrame via an Ibis memtable.

    Passing a raw DataFrame to ``create_table`` compiles a nameless
    ``DROP VIEW IF EXISTS`` (Ibis 12 + sqlglot 30). A memtable skips that path.

    ``overwrite=True`` on ``create_table`` still compiles a nameless
    ``DROP TABLE IF EXISTS``, so replacement is done with DuckDB SQL first.
    """
    if overwrite:
        conn.raw_sql(f'DROP TABLE IF EXISTS "{name}"')
    return conn.create_table(name, obj=ib.memtable(df), overwrite=False)
