import datetime
import json
import random
from dataclasses import dataclass
from typing import Final, Optional, Any

import arrow
import pandas as pd
from faker import Faker

from app.enums.constants import Constants
from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.lib.pd_save import save_sql_table_df
from app.utils.prints import print_to_terminal

faker: Final = Faker()


@dataclass
class QuickBooksCustomerDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - GivenName
    - FamilyName
    - PrimaryPhone
    - FullyQualifiedName
    - CurrencyRef
    - MetaData
    """
    business_id: int

    Id: Optional[int] = None

    SyncToken: Optional[str] = None
    DisplayName: Optional[str] = None
    Title: Optional[str] = None
    GivenName: Optional[str] = None
    MiddleName: Optional[str] = None
    Suffix: Optional[str] = None
    FamilyName: Optional[str] = None

    PrimaryEmailAddr: Optional[Any] = None
    ResaleNum: Optional[str] = None
    SecondaryTaxIdentifier: Optional[str] = None
    ARAccountRef: Optional[Any] = None
    DefaultTaxCodeRef: Optional[Any] = None
    PreferredDeliveryMethod: Optional[str] = None
    GSTIN: Optional[str] = None
    SalesTermRef: Optional[Any] = None
    CustomerTypeRef: Optional[Any] = None
    Fax: Optional[Any] = None
    BusinessNumber: Optional[str] = None
    BillWithParent: Optional[Any] = None
    CurrencyRef: Optional[Any] = None
    Mobile: Optional[Any] = None
    PrimaryPhone: Optional[Any] = None
    Job: Optional[bool] = None
    BalanceWithJobs: Optional[float] = None
    OpenBalanceDate: Optional[Any] = None
    AlternatePhone: Optional[Any] = None
    Taxable: Optional[bool] = None
    MetaData: Optional[Any] = None
    ParentRef: Optional[Any] = None
    Notes: Optional[str] = None
    WebAddr: Optional[Any] = None
    Active: Optional[bool] = True
    CompanyName: Optional[str] = None
    Balance: Optional[float] = None
    ShipAddr: Optional[Any] = None
    PaymentMethodRef: Optional[Any] = None
    IsProject: Optional[bool] = None
    Source: Optional[str] = None
    PrimaryTaxIdentifier: Optional[str] = None
    GSTRegistrationType: Optional[str] = None
    PrintOnCheckName: Optional[str] = None
    BillAddr: Optional[Any] = None
    FullyQualifiedName: Optional[str] = None
    Level: Optional[int] = None
    TaxExemptionReasonId: Optional[int] = None
    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def __generator__(self, customer_id: int):
        customer_name: str = random.choice([faker.name_male(), faker.name_female()])
        currency_ref: dict = random.choice(Constants.CUSTOMER_CURRENCIES.value)

        customer: QuickBooksCustomerDC = QuickBooksCustomerDC(
            Id=customer_id,
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),
            GivenName=random.choice([faker.first_name_female(), faker.first_name_male()]),
            DisplayName=customer_name,
            FamilyName=customer_name.split()[-1],
            Mobile=json.dumps({"FreeFormNumber": faker.phone_number()}),
            PrimaryPhone=json.dumps({"FreeFormNumber": faker.phone_number()}),
            FullyQualifiedName=customer_name,
            CurrencyRef=json.dumps({"name": currency_ref.get("full_name"), "value": currency_ref.get("short_name")}),
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return customer

    def generate(self, max_customers: int = 10) -> pd.DataFrame:
        print_to_terminal("Generating more customers...")

        existing_customers: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.customer} where business_id={self.business_id}", con=db_engine)
        existing_ids: set = set(existing_customers["Id"].values)

        try:
            max_customer_id: int = max(existing_ids)
        except ValueError:
            max_customer_id: int = 0

        # Customer accumulator
        customers: list = []

        for index in range(1, max_customers + 1):
            customer_id: int = (max_customer_id + index)
            customer: QuickBooksCustomerDC = QuickBooksCustomerDC(business_id=self.business_id).__generator__(
                customer_id=customer_id)
            customers.append(customer.__dict__)

        customers: pd.DataFrame = pd.DataFrame(data=customers)

        print_to_terminal("Done with generating more customers...")

        return customers

    def generate_and_save(self, max_customers: int = 10):
        print_to_terminal("Saving customers...")

        customer_objects = self.generate(max_customers=max_customers)
        save_sql_table_df(data=customer_objects, db_tablename=QuickBooksTable.customer)

        print_to_terminal("Done saving customers...")

        return customer_objects

# save_sql_table_df(data=generate_customer(business_id=1, max_customers=5), db_tablename="qb_customer")
