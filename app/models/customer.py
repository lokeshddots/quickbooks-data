import pandas as pd

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


class Customer:
    @staticmethod
    def get(business_id: int) -> pd.DataFrame:
        print_to_terminal("Retrieving customers from db...")

        customers: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.customer} where business_id={business_id} and Active=1", con=db_engine)

        print_to_terminal("Done retrieving customers from db...")

        return customers
