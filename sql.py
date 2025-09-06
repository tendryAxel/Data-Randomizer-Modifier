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
