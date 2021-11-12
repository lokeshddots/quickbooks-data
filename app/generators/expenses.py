import random
import traceback
from datetime import date
from typing import Final, Any, NoReturn

import pandas as pd
from faker import Faker

from app.dataclasses.bill import QuickBooksBillDC
from app.dataclasses.journal_entry import QuickBooksJournalEntryDC
from app.dataclasses.purchase import QuickBooksPurchaseDC
from app.dataclasses.vendor_credit import QuickBooksVendorCreditDC
from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.utils.prints import print_to_terminal
from app.utils.utils import flatten

faker: Final = Faker()


class GenerateExpenseData:
    def __init__(self, business_id: int, number_of_years: int = 2, single_record: bool = False, txn_date: date = None):
        self.business_id: int = business_id
        self.number_of_years: int = number_of_years
        self.single_record: bool = single_record
        self.txn_date: date = txn_date

        # Placeholders
        self.__qb_expense_tables__: Final = [QuickBooksTable.purchase, QuickBooksTable.vendor_credit,
                                             QuickBooksTable.bill, QuickBooksTable.journal_entry]
        self.__existing_expense_id__ = []
        self.__existing_expense_doc_id__ = []

        # After init
        self.__after_init__()

    def __after_init__(self) -> NoReturn:
        # Ensure business id is valid.
        business: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.business} where id={self.business_id}", con=db_engine)

        if business.empty:
            raise Exception(f"Invalid Business with id={self.business_id}")

        # Pre-Populate expense id hash-tables to ensure uniqueness
        self.__pre_populate_sales_identifiers__()

        return

    def __pre_populate_sales_identifiers__(self) -> NoReturn:
        # Fetch identifiers and update instance variables
        for qb_expense_table in self.__qb_expense_tables__:
            object_: pd.DataFrame = pd.read_sql_query(
                f"select * from {qb_expense_table} where business_id={self.business_id}", con=db_engine)

            if not object_.empty:
                # Update identifiers data-type
                object_["Id"] = pd.to_numeric(object_["Id"])

                # Append identifiers
                self.__existing_expense_id__.append(list(set(object_["Id"].values)))

        # Convert identifiers instance variables to hash tables
        self.__existing_expense_id__ = list(set(flatten(self.__existing_expense_id__)))

        return

    def __generate_unique_ids__(self) -> str:
        # Get max identifier value
        try:
            max_object_id = max(self.__existing_expense_id__)
        except ValueError:
            max_object_id = 0

        try:
            max_object_doc_id = max(self.__existing_expense_doc_id__)
        except ValueError:
            max_object_doc_id = 0

        # Compute new identifier values
        new_object_id = int(max_object_id) + 1
        new_object_doc_id = int(max_object_doc_id) + 1

        # Update instance variables
        self.__existing_expense_id__.append(new_object_id)
        self.__existing_expense_doc_id__.append(new_object_doc_id)

        return str(new_object_id)

    def __qb_table_selector__(self, qb_table: str) -> Any:
        selector: Any = {
            QuickBooksTable.purchase: QuickBooksPurchaseDC,
            QuickBooksTable.vendor_credit: QuickBooksVendorCreditDC,
            QuickBooksTable.bill: QuickBooksBillDC,
            QuickBooksTable.journal_entry: QuickBooksJournalEntryDC,
        }

        print_to_terminal(f"Working on {qb_table}...")

        # Generate new unique identifiers
        object_id = self.__generate_unique_ids__()

        # Generate transaction date
        txn_date: date = faker.date_between(start_date=f"-{self.number_of_years}y",
                                            end_date="today") if not self.single_record else self.txn_date

        # Run generator
        generator_object: Any = selector.get(qb_table)(business_id=self.business_id, Id=object_id,
                                                       TxnDate=txn_date).generate_and_save()

        print_to_terminal(f"Done working on {qb_table}...")

        return generator_object

    def run(self, min_txn_per_day: int = random.randrange(start=100, stop=200)) -> NoReturn:
        try:
            print_to_terminal("================ GENERATING QUICKBOOKS EXPENSE DATA ================")

            if self.single_record:
                for qb_record in self.__qb_expense_tables__:
                    self.__qb_table_selector__(qb_record)

            else:
                for index in range((365 * self.number_of_years) * min_txn_per_day + 1):
                    qb_table = random.choices(population=self.__qb_expense_tables__, weights=[0.55, 0.05, 0.3, 0.1])

                    print_to_terminal(f"<<<<<<<<<<<<<<<<<<<<{qb_table}>>>>>>>>>>>>>>>>>>>>>")

                    self.__qb_table_selector__(qb_table=qb_table[0])

            print_to_terminal("================ DONE GENERATING QUICKBOOKS EXPENSE DATA ================")

            return

        except Exception:
            traceback.print_exc()
