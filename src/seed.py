"""Load CSV data into the Postgres warehouse schema."""
from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import text

from .config import DATA_DIR
from .db import EngineFactory

TABLE_FILES = {
    "restaurants": DATA_DIR / "restaurants.csv",
    "couriers": DATA_DIR / "couriers.csv",
    "customers": DATA_DIR / "customers.csv",
    "orders": DATA_DIR / "orders.csv",
}

DBT_VIEW_NAMES = [
    "stg_orders",
    "stg_restaurants",
    "stg_couriers",
    "stg_customers",
    "fct_deliveries",
    "dim_restaurant",
    "dim_courier",
    "dim_customer",
    "kpi_delivery_overview",
    "monitoring_dq_exceptions",
]

DROP_VIEWS_SQLITE = [
    "DROP VIEW IF EXISTS stg_orders;",
    "DROP VIEW IF EXISTS stg_restaurants;",
    "DROP VIEW IF EXISTS stg_couriers;",
    "DROP VIEW IF EXISTS stg_customers;",
    "DROP VIEW IF EXISTS fct_deliveries;",
    "DROP VIEW IF EXISTS dim_restaurant;",
    "DROP VIEW IF EXISTS dim_courier;",
    "DROP VIEW IF EXISTS dim_customer;",
    "DROP VIEW IF EXISTS kpi_delivery_overview;",
    "DROP VIEW IF EXISTS monitoring_dq_exceptions;",
]


def _quote_ident(identifier: str) -> str:
    """Return a safely quoted SQL identifier."""

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def load_tables(factory: EngineFactory, table_files: Dict[str, Path] | None = None) -> Dict[str, int]:
    """Load CSVs into PostgreSQL using pandas.to_sql (similar to the notebook)."""

    files = table_files or TABLE_FILES
    engine = factory.get_engine()

    with factory.connect() as conn:
        if factory.dialect == "sqlite":
            for statement in DROP_VIEWS_SQLITE:
                conn.execute(text(statement))
        else:
            current_schema = conn.execute(text("SELECT current_schema()")).scalar_one()
            target_schemas = {current_schema}

            netid_schema = factory.settings.student_netid
            if netid_schema:
                target_schemas.add(netid_schema)

            search_path = conn.execute(text("SHOW search_path")).scalar_one()
            for part in search_path.split(","):
                cleaned = part.strip()
                if not cleaned:
                    continue
                if cleaned.startswith('"') and cleaned.endswith('"'):
                    cleaned = cleaned[1:-1]
                if cleaned.startswith("$"):
                    continue
                target_schemas.add(cleaned)

            existing_schemas = {
                row[0]
                for row in conn.execute(text("SELECT nspname FROM pg_catalog.pg_namespace"))
            }
            target_schemas &= existing_schemas
            if not target_schemas:
                target_schemas = {current_schema}

            for view in DBT_VIEW_NAMES:
                view_schemas = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT schemaname FROM pg_catalog.pg_views WHERE viewname = :view_name"
                        ),
                        {"view_name": view},
                    )
                }
                if not view_schemas:
                    view_schemas = target_schemas
                for schema in view_schemas:
                    if (
                        schema not in existing_schemas
                        or schema.startswith("pg_")
                        or schema == "information_schema"
                    ):
                        continue
                    conn.execute(
                        text(
                            f"DROP VIEW IF EXISTS {_quote_ident(schema)}.{_quote_ident(view)} CASCADE;"
                        )
                    )

            for table in files:
                table_schemas = {
                    row[0]
                    for row in conn.execute(
                        text(
                            "SELECT schemaname FROM pg_catalog.pg_tables WHERE tablename = :table_name"
                        ),
                        {"table_name": table},
                    )
                }
                if not table_schemas:
                    table_schemas = target_schemas
                for schema in table_schemas:
                    if (
                        schema not in existing_schemas
                        or schema.startswith("pg_")
                        or schema == "information_schema"
                    ):
                        continue
                    conn.execute(
                        text(
                            f"DROP TABLE IF EXISTS {_quote_ident(schema)}.{_quote_ident(table)} CASCADE;"
                        )
                    )
        conn.commit()

    row_counts: Dict[str, int] = {}
    for table, csv_path in files.items():
        df = pd.read_csv(csv_path)
        df.to_sql(table, con=engine, if_exists="replace", index=False)
        row_counts[table] = len(df)
    return row_counts


def verify_row_counts(factory: EngineFactory, tables: Dict[str, Path] | None = None) -> Dict[str, int]:
    files = tables or TABLE_FILES
    counts: Dict[str, int] = {}
    with factory.connect() as conn:
        for table in files:
            counts[table] = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
    return counts
