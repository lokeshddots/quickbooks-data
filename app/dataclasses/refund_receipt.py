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

from app.enums.constants import Constants
from app.helpers.line_details import SalesGroupItemLineDC, SalesItemLineDC
from app.helpers.quickbooks import QuickBooksTable
from app.lib.pd_save import save_sql_table_df
from app.models.account import Account
from app.models.business import Business
from app.models.customer import Customer
from app.models.department import Department
from app.models.payment_method import PaymentMethod

faker: Final = Faker()


@dataclass
class QuickBooksRefundReceiptDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Line
    - TxnDate
    - DocNumber
    - TxnTaxDetail
    - HomeBalance
    - TotalAmt
    - Balance
    - HomeTotalAmt
    """
    Id: int
    business_id: int

    DocNumber: str
    TxnDate: date

    TotalAmt: Optional[float] = 0
    Balance: Optional[float] = 0
    HomeBalance: Optional[float] = None
    HomeTotalAmt: Optional[float] = None
    Line: Optional[Any] = None
    TxnTaxDetail: Any = json.dumps({"TotalTax": 0.0})

    CustomerRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    BillEmail: Optional[Any] = None
    # ShipFromAddr: Optional[Any] = None
    CustomField: Optional[Any] = None
    # ShipDate: Optional[date] = None
    # TrackingNum: Optional[str] = None
    ClassRef: Optional[Any] = None
    PrintStatus: Optional[str] = random.choice(Constants.PRINT_STATUS.value)
    CheckPayment: Optional[Any] = None
    PaymentRefNum: Optional[str] = None
    TxnSource: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    TransactionLocationType: Optional[str] = None
    ApplyTaxAfterDiscount: Optional[bool] = False
    PrivateNote: Optional[str] = None
    DepositToAccountRef: Optional[Any] = None
    CustomerMemo: Optional[Any] = None
    # EmailStatus: Optional[str] = random.choice(Constants.EMAIL_STATUS.value)
    CreditCardPayment: Optional[Any] = None
    PaymentMethodRef: Optional[Any] = None
    ExchangeRate: Optional[float] = 1
    ShipAddr: Optional[Any] = None
    DepartmentRef: Optional[Any] = None
    # ShipMethodRef: Optional[Any] = None
    BillAddr: Optional[Any] = None
    MetaData: Optional[Any] = None
    # DeliveryInfo: Optional[Any] = None
    RecurDataRef: Optional[Any] = None
    # FreeFormAddress: Optional[bool] = False
    TaxExemptionRef: Optional[Any] = None

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

        # Get payment method ref...
        payment_method: pd.DataFrame = PaymentMethod.get(business_id=self.business_id)
        payment_method: pd.DataFrame = payment_method.loc[payment_method.Name == "Cash"]

        # Get business customers
        try:
            customer: pd.DataFrame = Customer.get(business_id=self.business_id).sample()
        except ValueError:
            raise ValueError(f"No customers available for business with id={self.business_id}")

        # Get DepositToAccountRef
        accounts_object: pd.DataFrame = Account.get(business_id=self.business_id)
        deposit_to_account: pd.DataFrame = accounts_object.loc[
            (accounts_object.AccountType == "Other Current Asset") | (accounts_object.AccountType == "Bank")].sample()

        # Get line details
        number_of_line_items = random.randrange(start=1, stop=5)
        include_group_item: bool = faker.boolean()

        if include_group_item:
            line = SalesGroupItemLineDC().generate(business_id=self.business_id,
                                                   number_of_line_items=number_of_line_items)
            line = SalesItemLineDC().generate(business_id=self.business_id,
                                              number_of_line_items=number_of_line_items) if line is None else line
        else:
            line = SalesItemLineDC().generate(business_id=self.business_id, number_of_line_items=number_of_line_items)

        # Get total amount from line items
        total_amount = round(sum([obj.get("Amount") for obj in line]), 2)

        # Compose response object
        refund_receipt: QuickBooksRefundReceiptDC = QuickBooksRefundReceiptDC(
            Id=self.Id,
            business_id=self.business_id,

            Line=json.dumps(line),
            DocNumber=self.DocNumber,
            TxnTaxDetail=self.TxnTaxDetail,
            TxnDate=self.TxnDate,

            HomeBalance=self.HomeBalance,
            Balance=self.Balance,
            TotalAmt=total_amount,
            HomeTotalAmt=self.HomeTotalAmt,

            SyncToken=str(random.randint(0, 4)),
            DepositToAccountRef=json.dumps(
                {"name": deposit_to_account.Name.item(), "value": deposit_to_account.Id.item()}),
            CustomerRef=json.dumps({"name": customer.DisplayName.item(), "value": customer.Id.item()}),
            CurrencyRef=json.dumps(
                {"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            DepartmentRef=json.dumps(
                {"name": department.Name.item(), "value": department.Id.item()}) if not department.empty else None,
            PaymentMethodRef=json.dumps({"name": payment_method.Name.item(), "value": payment_method.Id.item()}),
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()}),
            inserted_on=arrow.get(self.TxnDate).datetime

        )

        return refund_receipt

    def generate_and_save(self):
        refund_object = self.generate()
        save_sql_table_df(data=pd.DataFrame([refund_object.__dict__]), db_tablename=QuickBooksTable.refund_receipt)

        return refund_object
