import datetime
import random
import traceback
from typing import NoReturn

import arrow
import pandas as pd

from app.enums.constants import Constants
from app.generators.expenses import GenerateExpenseData
from app.generators.sales import GenerateSalesData
from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal


class PullTrackerFixture:
    def __init__(self, business_id: int, number_of_years: int = 2,
                 max_customers: int = random.randrange(start=100, stop=200),
                 max_departments: int = random.randrange(start=2, stop=10),
                 max_employees: int = random.randrange(start=2, stop=10),
                 max_vendors: int = random.randrange(start=2, stop=10)):
        self.business_id: int = business_id
        self.number_of_years: int = number_of_years
        self.max_customers: int = max_customers
        self.max_employees: int = max_employees
        self.max_vendors: int = max_vendors
        self.max_departments: int = max_departments

        self.__first_item_inserted_on: datetime = arrow.now().shift(years=-self.number_of_years).replace(month=1, day=1)
        self.__present_date: datetime = arrow.now()
        self.__qb_record_tables: list = [QuickBooksTable.sales_receipt, QuickBooksTable.invoice_receipt,
                                         QuickBooksTable.refund_receipt, QuickBooksTable.credit_memo,
                                         QuickBooksTable.bill, QuickBooksTable.purchase, QuickBooksTable.vendor_credit,
                                         QuickBooksTable.journal_entry]

    def __generate_last_record__(self) -> NoReturn:
        # Generate sales data
        GenerateSalesData(business_id=self.business_id, number_of_years=self.number_of_years,
                          max_customers=self.max_customers, max_departments=self.max_departments,
                          max_employees=self.max_employees, max_vendors=self.max_vendors, single_record=True,
                          txn_date=self.__first_item_inserted_on.date()).run()

        # Generate expense data
        GenerateExpenseData(business_id=self.business_id, number_of_years=self.number_of_years, single_record=True,
                            txn_date=self.__first_item_inserted_on.date()).run()

        return

    def __update_pull_tracker_table__(self) -> NoReturn:

        # Fetch records
        qb_pulled_records: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.pull_tracker} where business_id={self.business_id}", con=db_engine)

        # Update records
        for index in range(qb_pulled_records.shape[0]):
            record: pd.DataFrame = qb_pulled_records.loc[index]

            if record.id in self.__qb_record_tables:
                # Update records
                present_date: datetime = arrow.now().isoformat()
                record["first_item_inserted_on"] = self.__first_item_inserted_on.isoformat()
                record["last_item_inserted_on"] = self.__present_date.isoformat()
                record["last_pull_date_limit"] = self.__present_date.isoformat()

                # Update query and save records
                update_query = f"""
                    UPDATE {Constants.DATABASE_NAME.value}.{QuickBooksTable.pull_tracker}
                    SET 
                        first_item_inserted_on='{record.first_item_inserted_on}',
                        last_item_inserted_on='{record.last_item_inserted_on}',
                        last_pull_date_limit='{record.last_pull_date_limit}'
                    WHERE business_id={record.business_id} and id='{record.id}';
                """

                db_engine.execute(update_query)

        print_to_terminal("DONE UPDATING QB_PULL_TRACKER TABLE RECORDS FOR BOTH SALES AND EXPENSE RECORDS")

        return

    def run(self) -> NoReturn:
        try:
            print_to_terminal(
                "================ UPDATING QB_PULL_TRACKER TABLE RECORDS FOR BOTH SALES AND EXPENSE RECORDS ================")

            self.__update_pull_tracker_table__()
            self.__generate_last_record__()

            print_to_terminal(
                "================ DONE UPDATING QB_PULL_TRACKER TABLE RECORDS FOR BOTH SALES AND EXPENSE RECORDS ================")

            return

        except Exception:
            traceback.print_exc()
