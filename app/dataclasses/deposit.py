import datetime
import json
import random
from dataclasses import dataclass
from datetime import date
from typing import Final, Optional, Any

import arrow
import ccy
import pandas as pd
from faker import Faker

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.lib.pd_save import save_sql_table_df
from app.models.account import Account
from app.models.business import Business

faker: Final = Faker()


@dataclass
class QuickBooksDepositDC:
    """Below are required fields for successful record creation:
    - business_id
    - TotalAmt
    - HomeTotalAmt
    - Line
    - TxnDate
    - TxnTaxDetail
    """
    business_id: int

    TotalAmt: Optional[float] = 0
    HomeTotalAmt: Optional[float] = None
    Id: Optional[int] = None
    TxnTaxDetail: Optional[Any] = json.dumps({})
    TxnDate: Optional[date] = None
    Line: Optional[Any] = None

    DepositToAccountRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    PrivateNote: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    ExchangeRate: Optional[float] = 1.0
    DepartmentRef: Optional[Any] = None
    TxnSource: Optional[str] = None
    CashBack: Optional[Any] = None
    TransactionLocationType: Optional[str] = None

    MetaData: Optional[Any] = None
    RecurDataRef: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def generate(self, amount: float, deposit_date: date, payment_object_id: str):
        # Get existing deposit objects
        existing_deposit_objects: pd.DataFrame = pd.read_sql_query(f"select * from {QuickBooksTable.deposit} where business_id={self.business_id}", con=db_engine)
        existing_ids: set = set(existing_deposit_objects["Id"].values)

        try:
            max_id: int = max(existing_ids)
        except ValueError:
            max_id: int = 0

        object_id: str = str(max_id + 1)

        # Get the business details
        business_object: pd.DataFrame = Business.get(business_id=self.business_id)
        business_currency_code: str = business_object.currency.item()

        # Get DepositToAccountRef
        accounts_object: pd.DataFrame = Account.get(business_id=self.business_id)
        deposit_to_account: pd.DataFrame = accounts_object.loc[accounts_object.AccountType == "Bank"].sample()

        # Compose line object
        line: list = [{
            "Amount": amount,
            "LinkedTxn": [{
                "TxnId": payment_object_id,
                "TxnType": "Payment",
                "TxnLineId": "0"
            }]
        }]

        # Compose response object
        deposit: QuickBooksDepositDC = QuickBooksDepositDC(
            Id=int(object_id),
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),

            Line=json.dumps(line),
            TxnDate=deposit_date,
            TxnTaxDetail=self.TxnTaxDetail,
            TotalAmt=amount,
            HomeTotalAmt=self.HomeTotalAmt,

            DepositToAccountRef=json.dumps({"name": deposit_to_account.Name.item(), "value": deposit_to_account.Id.item()}),
            CurrencyRef=json.dumps({"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return deposit, object_id

    def generate_and_save(self, amount: float, deposit_date: date, payment_object_id: str) -> tuple:
        deposit_object, object_id = self.generate(amount=amount, deposit_date=deposit_date, payment_object_id=payment_object_id)
        save_sql_table_df(data=pd.DataFrame([deposit_object.__dict__]), db_tablename="qb_deposit")

        return deposit_object, object_id

# display(QuickBooksDepositDC(business_id=1).generate(amount=234.5, deposit_date=arrow.now().date(), payment_object_id="23"))
# save_sql_table_df(data=generate_department(business_id=1, max_department=5), db_tablename="qb_department")
