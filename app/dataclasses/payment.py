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
from app.models.payment_method import PaymentMethod

faker: Final = Faker()


@dataclass
class QuickBooksPaymentDC:
    """Below are required fields for successful record creation:
    - business_id
    - TotalAmt
    - Line
    - TxnDate
    """
    business_id: int

    Line: Optional[Any] = None

    TotalAmt: Optional[float] = 0
    Id: Optional[int] = None
    TxnDate: Optional[date] = None

    CustomerRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    PrivateNote: Optional[str] = None
    PaymentMethodRef: Optional[Any] = None
    UnappliedAmt: Optional[float] = False
    DepositToAccountRef: Optional[Any] = None
    ExchangeRate: float = 1.0
    TxnSource: Optional[str] = None
    ARAccountRef: Optional[Any] = None
    CreditCardPayment: Optional[Any] = None
    TransactionLocationType: Optional[str] = None
    PaymentRefNum: Optional[str] = None
    TaxExemptionRef: Optional[Any] = None
    MetaData: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def generate(self, amount: float, payment_date: date, customer_ref: dict, invoice_id: str,
                 is_deposit: bool = False):
        # Get existing deposit objects
        existing_deposit_objects: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.payment} where business_id={self.business_id}", con=db_engine)
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

        if is_deposit:
            deposit_to_account: pd.DataFrame = accounts_object.loc[accounts_object.AccountType == "Bank"].sample()
        else:
            deposit_to_account: pd.DataFrame = accounts_object.loc[
                (accounts_object.AccountType == "Other Current Asset") & (
                        accounts_object.AccountSubType == "UndepositedFunds")].sample()

        # Payment method
        payment_method: pd.DataFrame = PaymentMethod.get(business_id=self.business_id).sample()
        # payment_method: pd.DataFrame = payment_method.loc[payment_method.Name == "Cash"]

        # Compose line object
        line: list = [{
            "Amount": amount,
            "LinkedTxn": [
                {
                    "TxnId": invoice_id,
                    "TxnType": "Invoice"
                }
            ]
        }]

        # Compose response object
        payment: QuickBooksPaymentDC = QuickBooksPaymentDC(
            Id=int(object_id),
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),

            Line=json.dumps(line),
            TxnDate=payment_date,
            TotalAmt=amount,
            CustomerRef=json.dumps(customer_ref),
            PaymentMethodRef=json.dumps({"name": payment_method.Name.item(), "value": payment_method.Id.item()}),

            DepositToAccountRef=json.dumps(
                {"name": deposit_to_account.Name.item(), "value": deposit_to_account.Id.item()}),
            CurrencyRef=json.dumps(
                {"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return payment, object_id

    def generate_and_save(self, amount: float, payment_date: date, customer_ref: dict, invoice_id: str,
                          is_deposit: bool = False) -> tuple:
        payment_object, object_id = self.generate(amount=amount, payment_date=payment_date, customer_ref=customer_ref,
                                                  invoice_id=invoice_id, is_deposit=is_deposit)
        save_sql_table_df(data=pd.DataFrame([payment_object.__dict__]), db_tablename=QuickBooksTable.payment)

        return payment_object, object_id

# display(QuickBooksPaymentDC(business_id=1).generate(amount=23, payment_date=arrow.now().date(), customer_ref=None, invoice_id="23", is_deposit=True))
# save_sql_table_df(data=generate_department(business_id=1, max_department=5), db_tablename="qb_department")
