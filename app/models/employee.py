import pandas as pd

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


class Employee:
    @staticmethod
    def get(business_id: int) -> pd.DataFrame:
        print_to_terminal("Retrieving employees from db...")

        employee: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.employee} where business_id={business_id} and Active=1", con=db_engine)

        print_to_terminal("Done retrieving employees from db...")

        return employee
