import datetime
import json
import random
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Final, Optional, Any

import arrow
import pandas as pd
from faker import Faker

from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine
from app.lib.pd_save import save_sql_table_df
from app.utils.prints import print_to_terminal

faker: Final = Faker()


@dataclass
class QuickBooksEmployeeDC:
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
    PrimaryAddr: Optional[Any] = None
    PrimaryEmailAddr: Optional[Any] = None
    BillableTime: Optional[bool] = False
    GivenName: Optional[str] = None
    BirthDate: Optional[date] = None
    MiddleName: Optional[str] = None
    SSN: Optional[str] = None
    Gender: Optional[str] = None
    PrimaryPhone: Optional[Any] = None
    Active: Optional[bool] = True
    ReleasedDate: Optional[date] = None
    MetaData: Optional[Any] = None
    Mobile: Optional[Any] = None
    HiredDate: Optional[date] = None
    BillRate: Optional[float] = None
    Organization: Optional[str] = None
    Suffix: Optional[str] = None
    FamilyName: Optional[str] = None
    PrintOnCheckName: Optional[str] = None
    EmployeeNumber: Optional[str] = str(uuid.uuid4()).replace("-", "")
    V4IDPseudonym: Optional[str] = str(uuid.uuid4()).replace("-", "")

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def __generator__(self, employee_id: int):
        employee_name: str = random.choice([faker.name_male(), faker.name_female()])

        employee: QuickBooksEmployeeDC = QuickBooksEmployeeDC(
            Id=employee_id,
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),
            GivenName=random.choice([faker.first_name_female(), faker.first_name_male()]),
            DisplayName=employee_name,
            FamilyName=employee_name.split()[-1],
            Mobile=json.dumps({"FreeFormNumber": faker.phone_number()}),
            PrimaryPhone=json.dumps({"FreeFormNumber": faker.phone_number()}),
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return employee

    def generate(self, max_employees: int = 10) -> pd.DataFrame:
        print_to_terminal("Generating more employees...")

        existing_employees: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.employee} where business_id={self.business_id}", con=db_engine)
        existing_ids: set = set(existing_employees["Id"].values)

        try:
            max_employee_id: int = max(existing_ids)
        except ValueError:
            max_employee_id: int = 0

        # Employee accumulator
        employees: list = []

        for index in range(1, max_employees + 1):
            employee_id: int = (max_employee_id + index)
            employee: QuickBooksEmployeeDC = QuickBooksEmployeeDC(business_id=self.business_id).__generator__(
                employee_id=employee_id)
            employees.append(employee.__dict__)

        employees: pd.DataFrame = pd.DataFrame(data=employees)

        print_to_terminal("Done with generating more employees...")

        return employees

    def generate_and_save(self, max_employees: int = 10):
        print_to_terminal("Saving employees...")

        employee_objects = self.generate(max_employees=max_employees)
        save_sql_table_df(data=employee_objects, db_tablename=QuickBooksTable.employee)

        print_to_terminal("Done saving employees...")

        return employee_objects
