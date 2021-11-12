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
class QuickBooksVendorDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Balance
    """
    business_id: int

    Id: Optional[int] = 0

    Balance: Optional[float] = 0

    SyncToken: Optional[str] = None
    Title: Optional[str] = None
    GivenName: Optional[str] = None
    MiddleName: Optional[str] = None
    Suffix: Optional[str] = None
    FamilyName: Optional[str] = None
    DisplayName: Optional[str] = None
    CompanyName: Optional[str] = None
    PrimaryEmailAddr: Optional[Any] = None
    OtherContactInfo: Optional[Any] = None
    APAccountRef: Optional[Any] = None
    TermRef: Optional[Any] = None
    Source: Optional[str] = None
    GSTIN: Optional[str] = None
    T4AEligible: Optional[bool] = None
    Fax: Optional[Any] = None
    BusinessNumber: Optional[str] = None
    CurrencyRef: Optional[Any] = None
    HasTPAR: Optional[bool] = None
    TaxReportingBasis: Optional[str] = None
    Mobile: Optional[Any] = None
    PrimaryPhone: Optional[Any] = None
    Active: Optional[bool] = True
    Vendor1099: Optional[bool] = None
    T5018Eligible: Optional[bool] = None
    AlternatePhone: Optional[Any] = None
    BillRate: Optional[float] = None
    WebAddr: Optional[Any] = None
    VendorPaymentBankDetail: Optional[Any] = None
    TaxIdentifier: Optional[str] = None
    AcctNum: Optional[str] = None
    GSTRegistrationType: Optional[str] = None
    PrintOnCheckName: Optional[str] = None
    BillAddr: Optional[Any] = None
    MetaData: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def __generator__(self, vendor_id: int):
        vendor_name: str = random.choice([faker.name_male(), faker.name_female()])
        currency_ref: dict = random.choice(Constants.CUSTOMER_CURRENCIES.value)

        vendor: QuickBooksVendorDC = QuickBooksVendorDC(
            Id=vendor_id,
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),
            GivenName=random.choice([faker.first_name_female(), faker.first_name_male()]),
            DisplayName=vendor_name,
            FamilyName=vendor_name.split()[-1],
            PrimaryEmailAddr=json.dumps({"Address": f"{'_'.join(vendor_name.split()).lower()}@Intuit.com"}),
            Fax=json.dumps({"FreeFormNumber": faker.phone_number()}),
            PrintOnCheckName=vendor_name,
            Balance=round(
                float(random.choices(population=[0, random.randrange(start=50, stop=200)], weights=[0.8, 0.2])[0]), 2),

            Mobile=json.dumps({"FreeFormNumber": faker.phone_number()}),
            PrimaryPhone=json.dumps({"FreeFormNumber": faker.phone_number()}),
            CurrencyRef=json.dumps({"name": currency_ref.get("full_name"), "value": currency_ref.get("short_name")}),
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return vendor

    def generate(self, max_vendors: int = 10) -> pd.DataFrame:
        print_to_terminal("Generating more vendors...")

        existing_vendors: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.vendor} where business_id={self.business_id}", con=db_engine)
        existing_ids: set = set(existing_vendors["Id"].values)

        try:
            max_vendor_id: int = max(existing_ids)
        except ValueError:
            max_vendor_id: int = 0

        # Vendor accumulator
        vendors: list = []

        for index in range(1, max_vendors + 1):
            vendor_id: int = (max_vendor_id + index)
            vendor: QuickBooksVendorDC = QuickBooksVendorDC(business_id=self.business_id).__generator__(
                vendor_id=vendor_id)
            vendors.append(vendor.__dict__)

        vendors: pd.DataFrame = pd.DataFrame(data=vendors)

        print_to_terminal("Done with generating more vendors...")

        return vendors

    def generate_and_save(self, max_vendors: int = 10):
        print_to_terminal("Saving vendors...")

        vendor_objects = self.generate(max_vendors=max_vendors)
        save_sql_table_df(data=vendor_objects, db_tablename=QuickBooksTable.vendor)

        print_to_terminal("Done saving vendors...")

        return vendor_objects
