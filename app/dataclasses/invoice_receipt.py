import datetime
import json
import random
from dataclasses import dataclass
from datetime import date
from typing import Final, Optional, Any, Union

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
from app.utils.utils import run_fully_paid_invoice_payment_deposit_txn, run_partially_paid_invoice_payment_deposit_txn

faker: Final = Faker()


@dataclass
class QuickBooksInvoiceReceiptDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Line
    - LinkedTxn
    - TxnDate
    - DueDate
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
    LinkedTxn: Optional[Union[str, list]] = None
    TxnTaxDetail: Any = json.dumps({"TotalTax": 0.0})
    DueDate: Union[str, date] = arrow.now().date().isoformat()

    CustomerRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    BillEmail: Optional[Any] = None
    ShipFromAddr: Optional[Any] = None
    CustomField: Optional[Any] = None
    AllowOnlineCreditCardPayment: bool = False
    ShipDate: Optional[date] = None
    TrackingNum: Optional[str] = None
    ClassRef: Optional[Any] = None
    PrintStatus: Optional[str] = random.choice(Constants.PRINT_STATUS.value)
    # CheckPayment: Optional[Any] = None
    # PaymentRefNum: Optional[str] = None
    TxnSource: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    TransactionLocationType: Optional[str] = None
    ApplyTaxAfterDiscount: Optional[bool] = False
    PrivateNote: Optional[str] = None
    DepositToAccountRef: Optional[Any] = None
    CustomerMemo: Optional[Any] = None
    EmailStatus: Optional[str] = random.choice(Constants.EMAIL_STATUS.value)
    # CreditCardPayment: Optional[Any] = None
    # PaymentMethodRef: Optional[Any] = None
    ExchangeRate: Optional[float] = 1
    ShipAddr: Optional[Any] = None
    DepartmentRef: Optional[Any] = None
    ShipMethodRef: Optional[Any] = None
    BillAddr: Optional[Any] = None
    MetaData: Optional[Any] = None
    DeliveryInfo: Optional[Any] = None
    InvoiceLink: Optional[str] = None
    RecurDataRef: Optional[Any] = None
    FreeFormAddress: Optional[bool] = False
    TaxExemptionRef: Optional[Any] = None
    AllowIPNPayment: bool = False

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
            customer_ref: dict = {"name": customer.DisplayName.item(), "value": customer.Id.item()}
        except ValueError:
            raise ValueError(f"No customers available for business with id={self.business_id}")

        # Get DepositToAccountRef
        accounts_object: pd.DataFrame = Account.get(business_id=self.business_id)
        deposit_to_account: pd.DataFrame = accounts_object.loc[
            (accounts_object.AccountType == "Other Current Asset") | (accounts_object.AccountType == "Bank")].sample()

        # Compute DueDate
        max_month = random.randrange(start=3, stop=7)
        due_date: str = arrow.get(self.TxnDate).shift(months=max_month).date().isoformat()

        # Determine if invoice is fully paid or no
        fully_paid: bool = faker.boolean()
        is_deposited_payment: bool = faker.boolean()
        one_time_pay: bool = faker.boolean()
        started_payment: bool = faker.boolean()
        invoice_payment_done: int = random.randrange(start=1,
                                                     stop=9)  # This is the percentage amount paid already for this given invoice object.

        # Number of times a payment is associated to an invoice
        number_of_payments: int = random.randrange(start=1, stop=3)

        # Determine invoice payment dates
        late_payment: bool = faker.boolean()
        ontime_payment_date: date = faker.date_between_dates(date_start=arrow.get(self.TxnDate).date(),
                                                             date_end=arrow.get(due_date).date())
        late_payment_date_string = random.choice(["DAY", "WEEK", "MONTH", "YEAR"])
        late_payment_date_number = random.randrange(start=1, stop=5)

        if late_payment_date_string == "DAY":
            late_payment_date: date = faker.date_between_dates(date_start=arrow.get(due_date).date(),
                                                               date_end=arrow.get(due_date).shift(
                                                                   days=late_payment_date_number).date())
        elif late_payment_date_string == "WEEK":
            late_payment_date: date = faker.date_between_dates(date_start=arrow.get(due_date).date(),
                                                               date_end=arrow.get(due_date).shift(
                                                                   weeks=late_payment_date_number).date())
        elif late_payment_date_string == "MONTH":
            late_payment_date: date = faker.date_between_dates(date_start=arrow.get(due_date).date(),
                                                               date_end=arrow.get(due_date).shift(
                                                                   months=late_payment_date_number).date())
        else:
            late_payment_date: date = faker.date_between_dates(date_start=arrow.get(due_date).date(),
                                                               date_end=arrow.get(due_date).shift(years=1).date())

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
        invoice_amount = round(sum([obj.get("Amount") for obj in line]), 2)
        invoice_balance = invoice_amount

        # Compute payment and deposit objects.
        if fully_paid:
            # Payment of invoice is complete
            # run payment and deposit statistics
            payment_object_id, deposit_object_id, invoice_balance = run_fully_paid_invoice_payment_deposit_txn(
                business_id=self.business_id, customer_ref=customer_ref,
                late_payment_date_number=late_payment_date_number, invoice_id=str(self.Id),
                invoice_amount=invoice_amount, number_of_payments=number_of_payments, late_payment=late_payment,
                late_payment_date=late_payment_date, ontime_payment_date=ontime_payment_date, one_time_pay=one_time_pay,
                is_deposited_payment=is_deposited_payment)

        else:
            # Payment of invoice is either incomplete or not yet started...
            # run payment and deposit statistics
            payment_object_id, deposit_object_id, invoice_balance = run_partially_paid_invoice_payment_deposit_txn(
                business_id=self.business_id, customer_ref=customer_ref,
                late_payment_date_number=late_payment_date_number, invoice_id=str(self.Id),
                invoice_amount=invoice_amount, started_payment=started_payment,
                invoice_payment_done=invoice_payment_done, number_of_payments=number_of_payments,
                late_payment=late_payment, late_payment_date=late_payment_date, ontime_payment_date=ontime_payment_date,
                one_time_pay=one_time_pay, is_deposited_payment=is_deposited_payment)

        # Compute linked txn
        linked_txn: list = [{
            "TxnId": payment_object_id,
            "TxnType": "Payment"
        }] if payment_object_id is not None else []

        # Compose response object
        invoice_receipt: QuickBooksInvoiceReceiptDC = QuickBooksInvoiceReceiptDC(
            Id=self.Id,
            business_id=self.business_id,

            Line=json.dumps(line),
            LinkedTxn=json.dumps(linked_txn),
            DocNumber=self.DocNumber,
            TxnTaxDetail=self.TxnTaxDetail,
            TxnDate=self.TxnDate,
            DueDate=due_date,

            HomeBalance=self.HomeBalance,
            Balance=invoice_balance,
            TotalAmt=invoice_amount,
            HomeTotalAmt=self.HomeTotalAmt,

            SyncToken=str(random.randint(0, 4)),
            DepositToAccountRef=json.dumps(
                {"name": deposit_to_account.Name.item(), "value": deposit_to_account.Id.item()}),
            CustomerRef=json.dumps(customer_ref),
            CurrencyRef=json.dumps(
                {"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),

            DepartmentRef=json.dumps(
                {"name": department.Name.item(), "value": department.Id.item()}) if not department.empty else None,
            # PaymentMethodRef=json.dumps({"name": payment_method.Name.item(), "value": payment_method.Id.item()}),
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()}),
            inserted_on=arrow.get(self.TxnDate).datetime

        )

        return invoice_receipt

    def generate_and_save(self):
        invoice_object = self.generate()
        save_sql_table_df(data=pd.DataFrame([invoice_object.__dict__]), db_tablename=QuickBooksTable.invoice_receipt)

        return invoice_object
