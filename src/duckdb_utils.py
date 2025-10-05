import duckdb

def save_to_duckdb(*, users, contacts, utm, events, metrics, cat_mix, db_path: str):
    """
    Persist pandas DataFrames into a DuckDB file so the model is queryable via SQL.

    Parameters
    ----------
    users, contacts, utm, events, metrics, cat_mix : pandas.DataFrame
        DataFrames produced by the pipeline.
    db_path : str
        Path to the DuckDB file to create/overwrite, e.g. 'outputs/attribution.duckdb'.
    """
    con = duckdb.connect(db_path)

    # Registering temp views from DataFrames
    con.register("users_df", users)
    con.register("contacts_df", contacts)
    con.register("utm_df", utm)
    con.register("events_df", events)
    con.register("metrics_df", metrics)
    con.register("cat_mix_df", cat_mix)

    # Creating/replacing persistent tables
    con.execute("CREATE OR REPLACE TABLE users   AS SELECT * FROM users_df")
    con.execute("CREATE OR REPLACE TABLE contacts AS SELECT * FROM contacts_df")
    con.execute("CREATE OR REPLACE TABLE utm     AS SELECT * FROM utm_df")
    con.execute("CREATE OR REPLACE TABLE events  AS SELECT * FROM events_df")
    con.execute("CREATE OR REPLACE TABLE metrics AS SELECT * FROM metrics_df")
    con.execute("CREATE OR REPLACE TABLE cat_mix AS SELECT * FROM cat_mix_df")

    con.close()
    return db_path