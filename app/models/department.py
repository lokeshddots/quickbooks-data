import pandas as pd

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


class Department:
    @staticmethod
    def get(business_id: int) -> pd.DataFrame:
        print_to_terminal("Retrieving departments from db...")

        departments: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.department} where business_id={business_id} and Active=1", con=db_engine)

        print_to_terminal("Done retrieving departments from db...")

        return departments
