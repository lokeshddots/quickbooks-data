from enum import Enum
from typing import Final

import config


class Constants(Enum):
    DATABASE_URL: Final = config.DATABASE_URL
    DATABASE_NAME: Final = config.DATABASE_NAME
    CUSTOMER_CURRENCIES: Final = [
        {"full_name": "Euro", "short_name": "EUR", "symbol": "€"},
        {"full_name": "Indian Rupee", "short_name": "INR", "symbol": "₹"},
        {"full_name": "Pounds sterling", "short_name": "GBP", "symbol": "£"},
        {"full_name": "Russian ruble", "short_name": "RUB", "symbol": "₽"},
        {"full_name": "US dollar", "short_name": "USD", "symbol": "$"},
        {"full_name": "Canadian dollars", "short_name": "CAD", "symbol": "$"},
        {"full_name": "Nigerian naira", "short_name": "NGN", "symbol": "₦"}
    ]
    EMAIL_STATUS: Final = ["NotSet", "NeedToSend", "EmailSent"]
    PRINT_STATUS: Final = ["NotSet", "NeedToPrint", "PrintComplete"]
    PAYMENT_TYPE: Final = ["Cash", "Check", "CreditCard"]


class QuickBooksItemTypes(Enum):
    service: Final = "Service"
    inventory: Final = "Inventory"
    non_inventory: Final = "NonInventory"
    group: Final = "Group"
