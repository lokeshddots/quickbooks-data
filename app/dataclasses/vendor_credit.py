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

from app.helpers.line_details import AccountBasedLineDC, ItemBasedLineDC
from app.helpers.quickbooks import QuickBooksTable
from app.lib.pd_save import save_sql_table_df
from app.models.business import Business
from app.models.department import Department
from app.models.vendor import Vendor

faker: Final = Faker()


@dataclass
class QuickBooksVendorCreditDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Line
    - TxnDate
    - DocNumber
    - TotalAmt
    - Balance
    """
    Id: int
    business_id: int

    TxnDate: date

    TotalAmt: Optional[float] = 0
    Balance: Optional[float] = 0
    Line: Optional[Any] = None
    LinkedTxn: Optional[Any] = json.dumps([])
    DocNumber: str = str(uuid.uuid4()).replace("-", "")[:random.randrange(start=4, stop=7)]

    VendorRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    PrivateNote: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    ExchangeRate: Optional[float] = 1.0
    APAccountRef: Optional[Any] = None
    DepartmentRef: Optional[Any] = None
    IncludeInAnnualTPAR: Optional[bool] = False
    TransactionLocationType: Optional[str] = None
    MetaData: Optional[Any] = None
    RecurDataRef: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def generate(self):
        # Get the business details
        business_object: pd.DataFrame = Business.get(business_id=self.business_id)
        business_currency_code: str = business_object.currency.item()

        # Get a random department for the given business
        try:
            department: pd.DataFrame = Department.get(business_id=self.business_id).sample()
        except ValueError:
            department: pd.DataFrame = pd.DataFrame()

        # Get business vendors
        try:
            vendor: pd.DataFrame = Vendor.get(business_id=self.business_id).sample()
        except ValueError:
            raise ValueError(f"No vendors available for business with id={self.business_id}")

        # Get line details
        number_of_line_items = random.randrange(start=1, stop=5)
        include_item_line: bool = faker.boolean()
        lines: list = []

        lines.extend(
            AccountBasedLineDC().generate(business_id=self.business_id, number_of_line_items=number_of_line_items)
        )

        if include_item_line:
            lines.extend(
                ItemBasedLineDC().generate(business_id=self.business_id, number_of_line_items=number_of_line_items)
            )

        # Get total amount from line items
        total_amount = round(sum([obj.get("Amount") for obj in lines]), 2)

        # Compose response object
        vendor_credit: QuickBooksVendorCreditDC = QuickBooksVendorCreditDC(
            Id=self.Id,
            business_id=self.business_id,

            Line=json.dumps(lines),
            LinkedTxn=self.LinkedTxn,
            DocNumber=self.DocNumber,
            TxnDate=self.TxnDate,

            Balance=total_amount,
            TotalAmt=total_amount,

            SyncToken=str(random.randint(0, 4)),
            VendorRef=json.dumps({"name": vendor.DisplayName.item(), "value": vendor.Id.item()}),
            CurrencyRef=json.dumps(
                {"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            DepartmentRef=json.dumps(
                {"name": department.Name.item(), "value": department.Id.item()}) if not department.empty else None,
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()}),
            inserted_on=arrow.get(self.TxnDate).datetime
        )

        return vendor_credit

    def generate_and_save(self):
        vendor_credit_object = self.generate()
        save_sql_table_df(data=pd.DataFrame([vendor_credit_object.__dict__]),
                          db_tablename=QuickBooksTable.vendor_credit)

        return vendor_credit_object
