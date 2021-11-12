import json
import random
from datetime import date
from typing import NoReturn, List, Any, Optional, Tuple

import arrow
import pandas as pd

from app.dataclasses.deposit import QuickBooksDepositDC
from app.dataclasses.payment import QuickBooksPaymentDC
from app.helpers.quickbooks import QuickBooksTable
from app.lib.db_connection import db_engine


def flatten(payload: List[Any], accumulator: Optional[List[Any]] = None) -> List[Any]:
    """This function receives a python nested list and return a python list of 1-dimension (flatten).
    NB: When calling this function, you are not to pass the accumulator.
    Args:
        payload (List[Any]): This is the python list to be flatten
        accumulator (Optional[List[Any]]): This is our accumulator. Defaults to None.
    Returns:
        List[Any]: This is a flatten python list
    """
    if accumulator is None:
        accumulator: List[Any] = []

    for data in payload:
        if isinstance(data, list):
            flatten(data, accumulator)

        else:
            accumulator.append(data)

    return accumulator


def get_group_line_sales(business_id: int, generate: bool = False, group_qty: int = 1) -> dict:
    sale_tables = [QuickBooksTable.sales_receipt, QuickBooksTable.invoice_receipt, QuickBooksTable.credit_memo,
                   QuickBooksTable.refund_receipt]
    groups = {}

    def fix_group_details(payload: dict):
        payload_qty = payload.get("GroupLineDetail").get("Quantity")
        payload_lines = payload.get("GroupLineDetail").get("Line")
        amount = 0

        for payload_line in payload_lines:
            line_qty = payload_line.get("SalesItemLineDetail").get("Qty")
            line_item_unitprice = payload_line.get("SalesItemLineDetail").get("UnitPrice")

            original_line_qty = round(line_qty / payload_qty, 2)
            new_group_qty = round(original_line_qty * group_qty)
            new_amount = round(new_group_qty * line_item_unitprice, 2)

            payload_line["SalesItemLineDetail"]["Qty"] = new_group_qty
            payload_line["Amount"] = new_amount
            amount += new_amount

        payload["Amount"] = amount if amount > 0 else round(random.randrange(start=50, stop=150), 2)
        payload["GroupLineDetail"]["Quantity"] = group_qty

        return payload

    for sale_table in sale_tables:
        sales = pd.read_sql_query(f"select * from {sale_table} where business_id={business_id}", con=db_engine)

        for index in range(sales.shape[0]):
            sale = sales.loc[index]

            lines = json.loads(sale["Line"])
            group = [obj for obj in lines if obj.get("DetailType") == "GroupLineDetail"]

            if group:
                for group_obj in group:
                    # Append results
                    if generate:
                        groups[group_obj.get("GroupLineDetail").get("GroupItemRef").get("value")] = fix_group_details(
                            group_obj)
                    else:
                        groups[group_obj.get("GroupLineDetail").get("GroupItemRef").get("value")] = group_obj

    return groups


def run_fully_paid_invoice_payment_deposit_txn(business_id: int, customer_ref: dict, invoice_id: str,
                                               late_payment_date_number: int, invoice_amount: float,
                                               number_of_payments: int, late_payment: bool, late_payment_date: date,
                                               ontime_payment_date: date, one_time_pay: bool,
                                               is_deposited_payment: bool) -> Tuple[str, str, float]:
    invoice_balance: float = 0
    payment_object_id, deposit_object_id = None, None

    if late_payment:
        # This is an overdue invoice
        if one_time_pay:
            # Create single payment object

            if is_deposited_payment:
                # Create a payment & deposit objects
                payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, payment_date=late_payment_date, customer_ref=customer_ref,
                    invoice_id=invoice_id, is_deposit=True)
                deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, deposit_date=late_payment_date, payment_object_id=payment_object_id)

            else:
                # Continue as previewed
                # Create a payment & deposit objects
                payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, payment_date=late_payment_date, customer_ref=customer_ref,
                    invoice_id=invoice_id, is_deposit=False)
                deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, deposit_date=late_payment_date, payment_object_id=payment_object_id)

        else:
            # Create multiple payment objects
            sub_invoice_amount: float = round(invoice_amount / number_of_payments, 2)

            for index in range(number_of_payments):
                if is_deposited_payment:
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount, payment_date=late_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=True)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount,
                        deposit_date=arrow.get(late_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

                else:
                    # Continue as previewed
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount, payment_date=late_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=False)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount,
                        deposit_date=arrow.get(late_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

    else:
        # This is a not-due invoice

        if one_time_pay:
            # Create single payment object

            if is_deposited_payment:
                # Create a payment & deposit objects
                payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, payment_date=ontime_payment_date, customer_ref=customer_ref,
                    invoice_id=invoice_id, is_deposit=True)
                deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, deposit_date=ontime_payment_date, payment_object_id=payment_object_id)

            else:
                # Continue as previewed
                # Create a payment & deposit objects
                payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, payment_date=ontime_payment_date, customer_ref=customer_ref,
                    invoice_id=invoice_id, is_deposit=False)
                deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                    amount=invoice_amount, deposit_date=ontime_payment_date, payment_object_id=payment_object_id)

        else:
            # Create multiple payment objects
            sub_invoice_amount: float = round(invoice_amount / number_of_payments, 2)

            for index in range(number_of_payments):
                if is_deposited_payment:
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount, payment_date=ontime_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=True)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount,
                        deposit_date=arrow.get(ontime_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

                else:
                    # Continue as previewed
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount, payment_date=ontime_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=False)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=sub_invoice_amount,
                        deposit_date=arrow.get(ontime_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

    return payment_object_id, deposit_object_id, invoice_balance


def run_partially_paid_invoice_payment_deposit_txn(business_id: int, customer_ref: dict, invoice_id: str,
                                                   late_payment_date_number: int, invoice_amount: float,
                                                   started_payment: bool, invoice_payment_done: int,
                                                   number_of_payments: int, late_payment: bool, late_payment_date: date,
                                                   ontime_payment_date: date, one_time_pay: bool,
                                                   is_deposited_payment: bool) -> Tuple[str, str, float]:
    payment_object_id, deposit_object_id = None, None

    if late_payment:
        # This is an overdue invoice

        if started_payment:
            # Started with some payment
            invoice_amount_paid: float = round(((invoice_payment_done * 10) / 100) * invoice_amount, 2)
            invoice_balance: float = invoice_amount - invoice_amount_paid

            if one_time_pay:
                if is_deposited_payment:
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid, payment_date=late_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=True)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid,
                        deposit_date=arrow.get(late_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

                else:
                    # Continue as previewed
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid, payment_date=late_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=False)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid,
                        deposit_date=arrow.get(late_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

            else:
                # Create multiple payment objects
                sub_invoice_amount: float = round(invoice_amount_paid / number_of_payments, 2)

                for index in range(number_of_payments):
                    if is_deposited_payment:
                        # Create a payment & deposit objects
                        payment_object, payment_object_id = QuickBooksPaymentDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       payment_date=late_payment_date,
                                                                       customer_ref=customer_ref, invoice_id=invoice_id,
                                                                       is_deposit=True)
                        deposit_object, deposit_object_id = QuickBooksDepositDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       deposit_date=arrow.get(late_payment_date).shift(
                                                                           days=late_payment_date_number).date(),
                                                                       payment_object_id=payment_object_id)

                    else:
                        # Continue as previewed
                        # Create a payment & deposit objects
                        payment_object, payment_object_id = QuickBooksPaymentDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       payment_date=late_payment_date,
                                                                       customer_ref=customer_ref, invoice_id=invoice_id,
                                                                       is_deposit=False)
                        deposit_object, deposit_object_id = QuickBooksDepositDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       deposit_date=arrow.get(late_payment_date).shift(
                                                                           days=late_payment_date_number).date(),
                                                                       payment_object_id=payment_object_id)

        else:
            # No payment done so far.
            invoice_balance = invoice_amount

    else:
        # This is a not-due invoice

        if started_payment:
            # Started with some payment
            invoice_amount_paid: float = round(((invoice_payment_done * 10) / 100) * invoice_amount, 2)
            invoice_balance: float = invoice_amount - invoice_amount_paid

            if one_time_pay:
                if is_deposited_payment:
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid, payment_date=ontime_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=True)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid,
                        deposit_date=arrow.get(ontime_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

                else:
                    # Continue as previewed
                    # Create a payment & deposit objects
                    payment_object, payment_object_id = QuickBooksPaymentDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid, payment_date=ontime_payment_date, customer_ref=customer_ref,
                        invoice_id=invoice_id, is_deposit=False)
                    deposit_object, deposit_object_id = QuickBooksDepositDC(business_id=business_id).generate_and_save(
                        amount=invoice_amount_paid,
                        deposit_date=arrow.get(ontime_payment_date).shift(days=late_payment_date_number).date(),
                        payment_object_id=payment_object_id)

            else:
                # Create multiple payment objects
                sub_invoice_amount: float = round(invoice_amount_paid / number_of_payments, 2)

                for index in range(number_of_payments):
                    if is_deposited_payment:
                        # Create a payment & deposit objects
                        payment_object, payment_object_id = QuickBooksPaymentDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       payment_date=ontime_payment_date,
                                                                       customer_ref=customer_ref, invoice_id=invoice_id,
                                                                       is_deposit=True)
                        deposit_object, deposit_object_id = QuickBooksDepositDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       deposit_date=arrow.get(
                                                                           ontime_payment_date).shift(
                                                                           days=late_payment_date_number).date(),
                                                                       payment_object_id=payment_object_id)

                    else:
                        # Continue as previewed
                        # Create a payment & deposit objects
                        payment_object, payment_object_id = QuickBooksPaymentDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       payment_date=ontime_payment_date,
                                                                       customer_ref=customer_ref, invoice_id=invoice_id,
                                                                       is_deposit=False)
                        deposit_object, deposit_object_id = QuickBooksDepositDC(
                            business_id=business_id).generate_and_save(amount=sub_invoice_amount,
                                                                       deposit_date=arrow.get(
                                                                           ontime_payment_date).shift(
                                                                           days=late_payment_date_number).date(),
                                                                       payment_object_id=payment_object_id)

        else:
            # No payment done so far.
            invoice_balance = invoice_amount

    return payment_object_id, deposit_object_id, invoice_balance
