import traceback
from typing import NoReturn

import pandas as pd

from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


def save_sql_table_df(data: pd.DataFrame, db_tablename: str, if_exist: str = "append") -> NoReturn:
    try:
        print_to_terminal(f"Saving {db_tablename} records with shape={data.shape} to the database...")

        data.to_sql(name=db_tablename, con=db_engine, if_exists=if_exist, index=False)

        print_to_terminal(f"Done saving {db_tablename} records with shape={data.shape} to the database...")

    except Exception:
        traceback.print_exc()
