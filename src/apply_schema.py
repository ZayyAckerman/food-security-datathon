"""
Apply the PostgreSQL analytical view defined in schema.sql.
"""

import os
from pathlib import Path

from sqlalchemy import create_engine, text


def get_engine():
    """
    Create a SQLAlchemy engine using the same PostgreSQL
    environment variables used by the ETL load layer.
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


def apply_sql_schema():
    """
    Read schema.sql and create/update the analytical SQL view.
    """

    engine = get_engine()

    # schema.sql is located one level above src/
    project_root = Path(__file__).resolve().parent.parent
    schema_path = project_root / "schema.sql"

    if not schema_path.exists():
        raise FileNotFoundError(
            f"schema.sql not found at: {schema_path}"
        )

    sql_script = schema_path.read_text(encoding="utf-8")

    with engine.begin() as connection:
        connection.execute(text(sql_script))

    print(
        "✓ SQL view "
        "'view_eat_trade_empowerment_matrix' "
        "successfully created/updated in PostgreSQL!"
    )


if __name__ == "__main__":
    apply_sql_schema()