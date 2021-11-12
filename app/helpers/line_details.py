import json
import random
import traceback
from dataclasses import dataclass
from datetime import date
from typing import Optional, Any

import arrow
import pandas as pd

from app.enums.constants import QuickBooksItemTypes
from app.models.account import Account
from app.models.customer import Customer
from app.models.department import Department
from app.models.item import Item
from app.utils.utils import get_group_line_sales


@dataclass
class SalesItemLineDC:
    """Below are required fields for successful record creation:
    - Id
    - SalesItemLineDetail
    - Amount
    - Description
    """
    Id: Optional[str] = None
    SalesItemLineDetail: Optional[Any] = None
    Amount: Optional[float] = None
    Description: Optional[str] = None
    LineNum: Optional[float] = None

    DetailType: str = "SalesItemLineDetail"

    def generate(self, business_id: int, number_of_line_items: int = 1):
        sales_lines: list = []

        for index in range(number_of_line_items):
            # Retrieve sales item
            sales_item_detail, item_object = SalesItemLineDetailDC().generate(business_id=business_id)
            sales_item_detail: SalesItemLineDetailDC = sales_item_detail
            amount: float = round(sales_item_detail.UnitPrice * sales_item_detail.Qty, 2)
            amount: float = amount if amount > 0 else round(random.randrange(start=50, stop=150), 2)
            sales_item_detail: dict = sales_item_detail.__dict__

            # Compose sales line object.
            sales_line: SalesItemLineDC = SalesItemLineDC(
                Id=str(index + 1),
                SalesItemLineDetail=json.dumps(sales_item_detail),
                Amount=amount,
                Description=item_object.Description.item(),
                LineNum=float(index + 1)
            )

            # Append results
            sales_lines.append(sales_line.__dict__)

        return sales_lines


@dataclass
class SalesItemLineDetailDC:
    """Below are required fields for successful record creation:
    - ItemRef
    - ItemAccountRef
    - Qty
    - UnitPrice
    """
    ItemRef: Optional[Any] = None
    ItemAccountRef: Optional[Any] = None
    Qty: Optional[float] = None
    UnitPrice: Optional[float] = None

    TaxInclusiveAmt: Optional[float] = None
    DiscountAmt: Optional[float] = None
    ClassRef: Optional[Any] = None
    TaxCodeRef: Optional[Any] = json.dumps({"value": "NON"})
    MarkupInfo: Optional[Any] = None
    ServiceDate: Optional[date] = arrow.now().isoformat()
    DiscountRate: Optional[float] = None
    TaxClassificationRef: Optional[Any] = None

    def generate(self, business_id: int):
        # Retrieve sales item
        try:
            item_object: pd.DataFrame = Item.get(business_id=business_id)
            item_object: pd.DataFrame = item_object.loc[item_object.Type != QuickBooksItemTypes.group.value].sample()
        except ValueError:
            raise ValueError(f"No items found for the business with business_id={business_id}")

        item_account_ref: dict = item_object.IncomeAccountRef

        # Get line details
        sales_item_line_detail: SalesItemLineDetailDC = SalesItemLineDetailDC(
            ItemRef=json.dumps({"name": item_object.Name.item(), "value": item_object.Id.item()}),
            ItemAccountRef=json.dumps({"name": item_account_ref.get("name"), "value": item_account_ref.get("value")}),
            Qty=random.randrange(start=1, stop=5),
            UnitPrice=item_object.UnitPrice.item()
        )

        return sales_item_line_detail, item_object


@dataclass
class SalesGroupItemLineDC:
    """Below are required fields for successful record creation:
    - Id
    - GroupLineDetail
    - Description
    """
    Id: Optional[str] = None
    GroupLineDetail: Optional[Any] = None
    Description: Optional[str] = None
    LineNum: Optional[float] = None
    Amount: Optional[float] = None

    DetailType: str = "GroupLineDetail"

    def generate(self, business_id: int, number_of_line_items: int = 1):
        try:
            group_lines: list = []

            for index in range(number_of_line_items):

                # Get possible group items
                group_qty = random.randrange(start=1, stop=5)
                group_line_object: dict = get_group_line_sales(business_id=business_id, generate=True,
                                                               group_qty=group_qty)

                if group_line_object is None or not group_line_object:
                    return None

                group_line_object: dict = random.choice(list(group_line_object.values()))

                del group_line_object["Id"]
                del group_line_object["LineNum"]

                # Compose sales line object.
                group_line: SalesGroupItemLineDC = SalesGroupItemLineDC(
                    Id=str(index + 1),
                    LineNum=float(index + 1),
                    **group_line_object
                )

                # Append results
                group_lines.append(group_line.__dict__)

            return group_lines

        except Exception:
            traceback.print_exc()
            return None


@dataclass
class JournalEntryLineDetailDC:
    """Below are required fields for successful record creation:
    - PostingType
    - AccountRef
    """
    PostingType: Optional[str] = None

    JournalCodeRef: Optional[Any] = None
    AccountRef: Optional[Any] = None
    TaxApplicableOn: Optional[str] = None
    Entity: Optional[Any] = None
    TaxInclusiveAmt: Optional[float] = None
    ClassRef: Optional[Any] = None
    DepartmentRef: Optional[Any] = None
    TaxCodeRef: Optional[Any] = json.dumps({"value": "NON"})
    BillableStatus: Optional[str] = random.choice(["Billable", "NotBillable", "HasBeenBilled"])
    TaxAmount: Optional[float] = None

    def generate(self, business_id: int):
        # Get AccountRef
        account_ref: pd.DataFrame = Account.get(business_id=business_id).sample()

        # Posting Type
        posting_type: str = random.choice(["Credit", "Debit"])

        # Get a random department for the given business
        try:
            department: pd.DataFrame = Department.get(business_id=business_id).sample()
        except ValueError:
            department: pd.DataFrame = pd.DataFrame()

        # Get line details
        journal_line_detail: JournalEntryLineDetailDC = JournalEntryLineDetailDC(
            PostingType=posting_type,
            AccountRef=json.dumps({"name": account_ref.Name.item(), "value": account_ref.Id.item()}),
            DepartmentRef=json.dumps(
                {"name": department.Name.item(), "value": department.Id.item()}) if not department.empty else None,
        )

        return journal_line_detail


@dataclass
class JournalEntryLineDC:
    """Below are required fields for successful record creation:
    - Id
    - JournalEntryLineDetail
    - Amount
    - Description
    """
    Id: Optional[str] = None
    JournalEntryLineDetail: Optional[Any] = None
    Amount: Optional[float] = 0
    Description: Optional[str] = None
    LineNum: Optional[float] = None

    DetailType: str = "JournalEntryLineDetail"

    def generate(self, business_id: int, number_of_line_items: int = 1):
        journal_lines: list = []

        for index in range(number_of_line_items):
            # Retrieve journal item
            journal_item_detail: JournalEntryLineDetailDC = JournalEntryLineDetailDC().generate(business_id=business_id)
            journal_item_detail: dict = journal_item_detail.__dict__

            # Compose sales line object.
            journal_line: JournalEntryLineDC = JournalEntryLineDC(
                Id=str(index + 1),
                JournalEntryLineDetail=json.dumps(journal_item_detail),
                Amount=random.randrange(start=50, stop=200),
                LineNum=float(index + 1)
            )

            # Append results
            journal_lines.append(journal_line.__dict__)

        return journal_lines


@dataclass
class ItemBasedLineDC:
    """Below are required fields for successful record creation:
    - Id
    - ItemBasedExpenseLineDetail
    - Amount
    - LinkedTxn
    - Description
    """
    Id: Optional[str] = None
    ItemBasedExpenseLineDetail: Optional[Any] = None
    Amount: Optional[float] = 0
    LinkedTxn: Optional[Any] = None
    Description: Optional[str] = None
    LineNum: Optional[float] = None

    DetailType: str = "ItemBasedExpenseLineDetail"

    def generate(self, business_id: int, number_of_line_items: int = 1):
        item_base_lines: list = []

        for index in range(number_of_line_items):
            # Retrieve item line details
            item_line_detail: ItemBasedLineDetailDC = ItemBasedLineDetailDC().generate(business_id=business_id)
            item_qty: float = item_line_detail.Qty
            unit_price: float = item_line_detail.UnitPrice
            item_amount: float = round(item_qty * unit_price, 2)
            item_line_detail: dict = item_line_detail.__dict__

            # Compose sales line object.
            item_line: ItemBasedLineDC = ItemBasedLineDC(
                Id=str(index + 1),
                ItemBasedExpenseLineDetail=json.dumps(item_line_detail),
                Amount=item_amount,
                LineNum=float(index + 1)
            )

            # Append results
            item_base_lines.append(item_line.__dict__)

        return item_base_lines


@dataclass
class ItemBasedLineDetailDC:
    """Below are required fields for successful record creation:
    - PostingType
    - AccountRef
    """
    TaxInclusiveAmt: Optional[float] = 0
    ItemRef: Optional[Any] = None
    CustomerRef: Optional[Any] = None
    PriceLevelRef: Optional[Any] = None
    ClassRef: Optional[Any] = None
    TaxCodeRef: Optional[Any] = json.dumps({"value": "NON"})
    MarkupInfo: Optional[Any] = None
    BillableStatus: Optional[str] = random.choice(["Billable", "NotBillable", "HasBeenBilled"])
    Qty: Optional[float] = 1
    UnitPrice: Optional[float] = 0

    def generate(self, business_id: int):
        # Retrieve sales item
        try:
            item_object: pd.DataFrame = Item.get(business_id=business_id)
            item_object: pd.DataFrame = item_object.loc[item_object.Type != QuickBooksItemTypes.group.value].sample()
        except ValueError:
            raise ValueError(f"No items found for the business with business_id={business_id}")

        item_account_ref: dict = item_object.IncomeAccountRef

        # Get business customers
        try:
            customer: pd.DataFrame = Customer.get(business_id=business_id).sample()
        except ValueError:
            raise ValueError(f"No customers available for business with id={business_id}")

        # Get line details
        item_line_detail: ItemBasedLineDetailDC = ItemBasedLineDetailDC(
            CustomerRef=json.dumps({"name": customer.DisplayName.item(), "value": customer.Id.item()}),
            ItemRef=json.dumps({"name": item_object.Name.item(), "value": item_object.Id.item()}),
            Qty=random.randrange(start=1, stop=5),
            UnitPrice=item_object.UnitPrice.item()
        )

        return item_line_detail


@dataclass
class AccountBasedLineDC:
    """Below are required fields for successful record creation:
    - Id
    - ItemBasedExpenseLineDetail
    - Amount
    - LinkedTxn
    - Description
    """
    Id: Optional[str] = None
    AccountBasedExpenseLineDetail: Optional[Any] = None
    Amount: Optional[float] = 0
    Description: Optional[str] = None
    LineNum: Optional[float] = None

    DetailType: str = "AccountBasedExpenseLineDetail"

    def generate(self, business_id: int, number_of_line_items: int = 1):
        account_base_lines: list = []

        for index in range(number_of_line_items):
            # Retrieve item line details
            account_line_detail: AccountBasedLineDetailDC = AccountBasedLineDetailDC().generate(business_id=business_id)
            account_line_detail: dict = account_line_detail.__dict__

            # Compose sales line object.
            account_line: AccountBasedLineDC = AccountBasedLineDC(
                Id=str(index + 1),
                AccountBasedExpenseLineDetail=json.dumps(account_line_detail),
                Amount=round(random.randrange(start=100, stop=500), 2),
                LineNum=float(index + 1)
            )

            # Append results
            account_base_lines.append(account_line.__dict__)

        return account_base_lines


@dataclass
class AccountBasedLineDetailDC:
    """Below are required fields for successful record creation:
    - PostingType
    - AccountRef
    """
    TaxAmount: Optional[float] = 0
    AccountRef: Optional[Any] = None
    CustomerRef: Optional[Any] = None
    TaxInclusiveAmt: Optional[float] = None
    ClassRef: Optional[Any] = None
    TaxCodeRef: Optional[Any] = json.dumps({"value": "NON"})
    MarkupInfo: Optional[Any] = None
    BillableStatus: Optional[str] = random.choice(["Billable", "NotBillable", "HasBeenBilled"])

    def generate(self, business_id: int):
        # Get AccountRef
        accounts_object: pd.DataFrame = Account.get(business_id=business_id)
        account_ref: pd.DataFrame = accounts_object.loc[
            (accounts_object.AccountType == "Expense") | (accounts_object.AccountType == "Other Expense")].sample()

        # Get business customers
        try:
            customer: pd.DataFrame = Customer.get(business_id=business_id).sample()
        except ValueError:
            raise ValueError(f"No customers available for business with id={business_id}")

        # Get line details
        account_line_detail: AccountBasedLineDetailDC = AccountBasedLineDetailDC(
            AccountRef=json.dumps({"name": account_ref.Name.item(), "value": account_ref.Id.item()}),
            CustomerRef=json.dumps({"name": customer.DisplayName.item(), "value": customer.Id.item()})
        )

        return account_line_detail
