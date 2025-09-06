import os
import dotenv
import json

import pandas as pd
import sqlalchemy
from termcolor import colored

dotenv.load_dotenv()
MOCK_JSON_FILE = "mock.json"

def load_table(table_name) -> pd.DataFrame:
    if os.getenv("IS_TESTING"): return load_mock_table(table_name)
    return pd.read_sql_table(table_name, sqlalchemy.create_engine(os.getenv("DB_URL")))


def load_mock_table(table_name) -> pd.DataFrame:
    used_table_name = table_name
    with open(MOCK_JSON_FILE) as file_data:
        data: dict[str, str] = json.load(file_data)
    if table_name not in data.keys():
        print(colored(f"table '{table_name}' not found in mock, 'user' table will be used by default", "yellow"))
        used_table_name = "user"
    return pd.DataFrame(data.get(used_table_name))


def dataframe_to_pg_bulk_update(
        df: pd.DataFrame,
        table_name: str,
        key_column: str,
        cast_map: dict[str, str] = None
) -> str:
    """
    Generates a one-shot PostgreSQL UPDATE using VALUES and FROM.

    Parameters:
        df (pd.DataFrame): DataFrame with update data.
        table_name (str): Target SQL table name.
        key_column (str): Column used as join key (usually a primary key or unique constraint).
        cast_map (dict): Optional dict mapping column names to PostgreSQL types (for ::type casts).

    Returns:
        str: A PostgreSQL UPDATE statement.
    """

    if key_column not in df.columns:
        raise ValueError(f"Key column '{key_column}' not found in DataFrame")

    update_columns = [col for col in df.columns if col != key_column]
    if not update_columns:
        raise ValueError("DataFrame must have at least one column to update besides the key")

    # Generate the VALUES list
    values_clause = ",\n    ".join(
        "(" + ", ".join(
            _sql_literal(row[col], cast=(cast_map or {}).get(col))
            for col in df.columns
        ) + ")"
        for _, row in df.iterrows()
    )

    # Build the alias column list
    alias_columns = ", ".join(df.columns)

    # Build the SET clause
    set_clause = ",\n    ".join(
        f"{col} = data.{col}" for col in update_columns
    )

    # Final SQL
    sql = f"""UPDATE {table_name}
SET
    {set_clause}
FROM (
    VALUES 
    {values_clause}
) AS data({alias_columns})
WHERE {table_name}.{key_column} = data.{key_column};"""

    return sql

def _sql_literal(val, cast: str = None) -> str:
    """Helper to format SQL literals safely for PostgreSQL"""
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, str):
        val = val.replace("'", "''")  # Escape single quotes
        return f"'{val}'{f'::{cast}' if cast else ''}"
    else:
        return f"{val}{f'::{cast}' if cast else ''}"

