import pandas as pd

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


class Business:
    @staticmethod
    def get(business_id: int) -> pd.DataFrame:
        print_to_terminal("Retrieving business from db...")

        business: pd.DataFrame = pd.read_sql_query(f"select * from {QuickBooksTable.business} where id={business_id} and active=1", con=db_engine)

        print_to_terminal("Done retrieving business from db...")

        return business
