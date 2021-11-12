import datetime
import json
import random
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Final, Optional, Any

import arrow
import ccy
import pandas as pd
from faker import Faker

from app.helpers.line_details import JournalEntryLineDC
from app.helpers.quickbooks import QuickBooksTable
from app.lib.pd_save import save_sql_table_df
from app.models.business import Business

faker: Final = Faker()


@dataclass
class QuickBooksJournalEntryDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Line
    - TxnDate
    - DocNumber
    - TotalAmt
    - HomeTotalAmt
    """
    Id: int
    business_id: int

    TxnDate: date

    TotalAmt: Optional[float] = 0
    HomeTotalAmt: Optional[float] = 0
    Line: Optional[Any] = None
    TxnTaxDetail: Optional[Any] = json.dumps({})
    DocNumber: str = str(uuid.uuid4()).replace("-", "")[:random.randrange(start=4, stop=7)]

    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    PrivateNote: Optional[str] = "Opening Balance"
    ExchangeRate: Optional[float] = 1.0
    TaxRateRef: Optional[Any] = None
    TransactionLocationType: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    Adjustment: Optional[bool] = False
    RecurDataRef: Optional[Any] = None
    MetaData: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def generate(self):
        # Get the business details
        business_object: pd.DataFrame = Business.get(business_id=self.business_id)
        business_currency_code: str = business_object.currency.item()

        # Get line details
        number_of_line_items = random.randrange(start=1, stop=5)
        line = JournalEntryLineDC().generate(business_id=self.business_id, number_of_line_items=number_of_line_items)

        # Get total amount from line items
        total_amount = round(sum([obj.get("Amount") for obj in line]), 2)

        # Compose response object
        journal_entry: QuickBooksJournalEntryDC = QuickBooksJournalEntryDC(
            Id=self.Id,
            business_id=self.business_id,

            Line=json.dumps(line),
            DocNumber=self.DocNumber,
            TxnDate=self.TxnDate,

            TotalAmt=total_amount,

            SyncToken=str(random.randint(0, 4)),
            CurrencyRef=json.dumps(
                {"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()}),
            inserted_on=arrow.get(self.TxnDate).datetime
        )

        return journal_entry

    def generate_and_save(self):
        journal_entry_object = self.generate()
        save_sql_table_df(data=pd.DataFrame([journal_entry_object.__dict__]),
                          db_tablename=QuickBooksTable.journal_entry)

        return journal_entry_object
