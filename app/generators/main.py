import random
from typing import NoReturn

from app.generators.expenses import GenerateExpenseData
from app.generators.sales import GenerateSalesData
from app.helpers.pull_tracker_fixture import PullTrackerFixture


class QuickBooksGenerator:
    def __init__(self, business_id: int, number_of_years: int = 2,
                 max_customers: int = random.randrange(start=100, stop=200),
                 max_departments: int = random.randrange(start=2, stop=10),
                 max_employees: int = random.randrange(start=2, stop=10),
                 max_vendors: int = random.randrange(start=2, stop=10)):
        self.business_id: int = business_id
        self.number_of_years: int = number_of_years
        self.max_customers: int = max_customers
        self.max_employees: int = max_employees
        self.max_vendors: int = max_vendors
        self.max_departments: int = max_departments

    def run(self) -> NoReturn:
        # Generate sales data
        GenerateSalesData(business_id=self.business_id, number_of_years=self.number_of_years,
                          max_customers=self.max_customers, max_departments=self.max_departments,
                          max_employees=self.max_employees, max_vendors=self.max_vendors).run()

        # Generate expense data
        GenerateExpenseData(business_id=self.business_id, number_of_years=self.number_of_years).run()

        # Update pull tracker table
        PullTrackerFixture(business_id=self.business_id, number_of_years=self.number_of_years,
                           max_customers=self.max_customers, max_departments=self.max_departments,
                           max_employees=self.max_employees, max_vendors=self.max_vendors).run()

        return
