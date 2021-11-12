import datetime
import json
import random
from dataclasses import dataclass
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
class QuickBooksDepartmentDC:
    """Below are required fields for successful record creation:
    - Id
    - business_id
    - Name
    - SyncToken
    - FullyQualifiedName
    - MetaData
    """
    business_id: int

    Id: Optional[int] = None

    Name: Optional[str] = None
    FullyQualifiedName: Optional[str] = None
    SyncToken: Optional[str] = None
    ParentRef: Optional[Any] = None
    SubDepartment: bool = False
    Active: bool = True
    MetaData: Optional[Any] = None

    inserted_on: Optional[Any] = datetime.datetime.today()
    created_on: Optional[Any] = datetime.datetime.today()
    updated_on: Optional[Any] = datetime.datetime.today()

    def __generator__(self, department_id: int):
        department_name: str = faker.city()
        department: QuickBooksDepartmentDC = QuickBooksDepartmentDC(
            Id=department_id,
            business_id=self.business_id,
            SyncToken=str(random.randint(0, 4)),
            Name=department_name.split()[0],
            FullyQualifiedName=department_name,
            MetaData=json.dumps({"CreateTime": arrow.now().isoformat(), "LastUpdatedTime": arrow.now().isoformat()})
        )

        return department

    def generate(self, max_departments: int = 10) -> pd.DataFrame:
        print_to_terminal("Generating more departments...")

        existing_departments: pd.DataFrame = pd.read_sql_query(
            f"select * from {QuickBooksTable.department} where business_id={self.business_id}", con=db_engine)
        existing_ids: set = set(existing_departments["Id"].values)

        try:
            max_department_id: int = max(existing_ids)
        except ValueError:
            max_department_id: int = 0

        # Department accumulator
        departments: list = []

        for index in range(1, max_departments + 1):
            department_id: int = (max_department_id + index)
            department: QuickBooksDepartmentDC = QuickBooksDepartmentDC(business_id=self.business_id).__generator__(
                department_id=department_id)
            departments.append(department.__dict__)

        departments: pd.DataFrame = pd.DataFrame(data=departments)

        print_to_terminal("Done with generating more departments...")

        return departments

    def generate_and_save(self, max_departments: int = 10):
        print_to_terminal("Saving departments...")

        department_objects = self.generate(max_departments=max_departments)
        save_sql_table_df(data=department_objects, db_tablename=QuickBooksTable.department)

        print_to_terminal("Done saving departments...")

        return department_objects
