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

from app.enums.constants import Constants
from app.helpers.line_details import AccountBasedLineDC, ItemBasedLineDC
from app.helpers.quickbooks import QuickBooksTable, EntityRefDC
from app.lib.pd_save import save_sql_table_df
from app.models.account import Account
from app.models.business import Business
from app.models.customer import Customer
from app.models.department import Department
from app.models.employee import Employee
from app.models.vendor import Vendor

faker: Final = Faker()


@dataclass
class QuickBooksPurchaseDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Line
    - TxnDate
    - DocNumber
    - HomeBalance
    - TotalAmt
    - Balance
    """
    Id: int
    business_id: int

    TxnDate: date

    TotalAmt: Optional[float] = 0
    Line: Optional[Any] = None
    LinkedTxn: Optional[Any] = json.dumps([])
    TxnTaxDetail: Optional[Any] = None
    DocNumber: str = str(uuid.uuid4()).replace("-", "")[:random.randrange(start=4, stop=7)]

    PaymentType: Optional[str] = None
    AccountRef: Optional[Any] = None
    SyncToken: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    PrintStatus: Optional[str] = random.choice(Constants.PRINT_STATUS.value)
    RemitToAddr: Optional[Any] = None
    TxnSource: Optional[str] = None
    GlobalTaxCalculation: Optional[str] = None
    TransactionLocationType: Optional[str] = None
    MetaData: Optional[Any] = None
    PrivateNote: Optional[str] = None
    Credit: Optional[bool] = None
    PaymentMethodRef: Optional[Any] = None
    PurchaseEx: Optional[Any] = None
    ExchangeRate: Optional[float] = 1.0
    DepartmentRef: Optional[Any] = None
    EntityRef: Optional[Any] = None
    IncludeInAnnualTPAR: Optional[bool] = False
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

        # Get payment type
        payment_type: str = random.choice(Constants.PAYMENT_TYPE.value)
        is_credit: bool = faker.boolean()

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

        # Get AccountRef
        accounts_object: pd.DataFrame = Account.get(business_id=self.business_id)

        if payment_type == "CreditCard":
            try:
                account_ref: pd.DataFrame = accounts_object.loc[accounts_object.AccountType == "Credit Card"].sample()
            except Exception:
                account_ref: pd.DataFrame = accounts_object.loc[(accounts_object.AccountType == "Bank") | (accounts_object.AccountType == "Fixed Asset") | (accounts_object.AccountType == "Other Current Asset") | (accounts_object.AccountType == "Expense") | (accounts_object.AccountType == "Other Expense")].sample()

        else:
            account_ref: pd.DataFrame = accounts_object.loc[(accounts_object.AccountType == "Bank") | (accounts_object.AccountType == "Fixed Asset") | (accounts_object.AccountType == "Other Current Asset") | (accounts_object.AccountType == "Expense") | (accounts_object.AccountType == "Other Expense")].sample()
            is_credit = None

        # Determine EntityRef
        have_entity_ref: bool = faker.boolean()
        entity_ref = None

        if have_entity_ref:
            entity_type: str = random.choice([EntityRefDC.vendor, EntityRefDC.customer, EntityRefDC.employee])

            if entity_type == EntityRefDC.employee:
                # Get business employee
                try:
                    employee: pd.DataFrame = Employee.get(business_id=self.business_id).sample()
                    entity_ref = json.dumps({"name": employee.DisplayName.item(), "value": employee.Id.item(), "type": entity_type})

                except ValueError:
                    pass

            elif entity_type == EntityRefDC.customer:
                # Get business customer
                try:
                    customer: pd.DataFrame = Customer.get(business_id=self.business_id).sample()
                    entity_ref = json.dumps({"name": customer.DisplayName.item(), "value": customer.Id.item(), "type": entity_type})

                except ValueError:
                    pass

            elif entity_type == EntityRefDC.vendor:
                # Get business employee
                try:
                    vendor: pd.DataFrame = Vendor.get(business_id=self.business_id).sample()
                    entity_ref = json.dumps({"name": vendor.DisplayName.item(), "value": vendor.Id.item(), "type": entity_type})

                except ValueError:
                    pass

        # Compose response object
        purchase: QuickBooksPurchaseDC = QuickBooksPurchaseDC(
            Id=self.Id,
            business_id=self.business_id,

            Line=json.dumps(lines),
            LinkedTxn=self.LinkedTxn,
            DocNumber=self.DocNumber,
            TxnDate=self.TxnDate,

            PaymentType=payment_type,
            Credit=is_credit,
            EntityRef=entity_ref,

            TotalAmt=total_amount,

            SyncToken=str(random.randint(0, 4)),
            CurrencyRef=json.dumps({"name": ccy.currency(code=business_currency_code).name, "value": business_currency_code}),
            AccountRef=json.dumps({"name": account_ref.Name.item(), "value": account_ref.Id.item()}),

            DepartmentRef=json.dumps({"name": department.Name.item(), "value": department.Id.item()}) if not department.empty else None,
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()}),
            inserted_on=arrow.get(self.TxnDate).datetime
        )

        return purchase

    def generate_and_save(self):
        purchase_object = self.generate()
        save_sql_table_df(data=pd.DataFrame([purchase_object.__dict__]), db_tablename=QuickBooksTable.purchase)

        return purchase_object
