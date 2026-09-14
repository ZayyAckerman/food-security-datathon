"""
Loading layer of the ETL pipeline.

Writes analysis-ready tables to PostgreSQL using pandas' `.to_sql`.

Connection settings are read from environment variables so credentials
are not hardcoded in source (see .env.example).

The transformation layer produces multiple analysis-ready tables,
which are loaded into PostgreSQL individually.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from typing import Literal


# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

def get_engine():
    """
    Create a SQLAlchemy engine using PostgreSQL connection settings
    stored in environment variables.
    """

    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "postgres")
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5433")
    db = os.environ.get("PG_DATABASE", "food_datathon")

    url = (
        f"postgresql+psycopg2://"
        f"{user}:{password}@{host}:{port}/{db}"
    )

    return create_engine(url)


# ---------------------------------------------------------------------------
# Load a single table
# ---------------------------------------------------------------------------

def load_table(
    df: pd.DataFrame,
    table_name: str,
    engine=None,
    if_exists: Literal["fail", "replace", "append", "delete_rows"] = "replace"
) -> None:
    """
    Load a single DataFrame into PostgreSQL.

    if_exists="replace" is used because the pipeline's source data is a
    periodically refreshed research snapshot rather than an incremental log.

    Each pipeline run therefore represents the latest known state instead
    of accumulating duplicate rows.
    """

    engine = engine or get_engine()

    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False
    )

    print(
        f"load_table: wrote {len(df)} rows "
        f"to '{table_name}'"
    )


# ---------------------------------------------------------------------------
# Load all analysis tables
# ---------------------------------------------------------------------------

def load_all(
    tables: dict,
    engine=None
) -> None:
    """
    Load all analysis-ready tables into PostgreSQL.

    Parameters
    ----------
    tables : dict
        Dictionary returned by build_analysis_tables(), where each key
        is the PostgreSQL table name and each value is a DataFrame.
    """

    engine = engine or get_engine()

    for name, df in tables.items():
        load_table(
            df,
            name,
            engine=engine
        )


# ---------------------------------------------------------------------------
# Run complete ETL pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    from extract import (
        extract_dbm_data,
        extract_sa_province_data,
        extract_empowerment_data,
        extract_trade_data
    )

    from transform import build_analysis_tables

    # -------------------------------------------------------
    # Extract
    # -------------------------------------------------------

    dbm = extract_dbm_data()
    sa = extract_sa_province_data()
    empowerment = extract_empowerment_data()
    trade = extract_trade_data()

    print("✓ Extraction successful")

    # -------------------------------------------------------
    # Transform
    # -------------------------------------------------------

    tables = build_analysis_tables(
        dbm,
        sa,
        empowerment,
        trade
    )

    print("✓ Transformation successful")

    # -------------------------------------------------------
    # Load
    # -------------------------------------------------------

    load_all(tables)

    print("✓ All tables loaded to PostgreSQL successfully!")