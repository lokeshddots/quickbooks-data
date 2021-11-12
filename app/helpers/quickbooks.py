from dataclasses import dataclass
from typing import Final


@dataclass
class QuickBooksTable:
    business: Final = "business"

    sales_receipt: Final = "qb_salesreceipt"
    invoice_receipt: Final = "qb_invoice"
    refund_receipt: Final = "qb_refundreceipt"
    credit_memo: Final = "qb_creditmemo"

    payment: Final = "qb_payment"
    deposit: Final = "qb_deposit"
    item: Final = "qb_item"
    customer: Final = "qb_customer"
    department: Final = "qb_department"
    payment_method: Final = "qb_paymentmethod"
    vendor_credit: Final = "qb_vendorcredit"
    account: Final = "qb_account"
    vendor: Final = "qb_vendor"
    bill: Final = "qb_bill"
    purchase: Final = "qb_purchase"
    journal_entry: Final = "qb_journalentry"
    employee: Final = "qb_employee"

    pull_tracker: Final = "qb_pull_tracker"


@dataclass
class EntityRefDC:
    customer: Final = "Customer"
    employee: Final = "Employee"
    vendor: Final = "Vendor"
