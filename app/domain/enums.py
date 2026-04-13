from enum import Enum


class IntentType(str, Enum):
    PRICE = "price"
    CONSULTATION_INTEREST = "consultation_interest"
    BOOKING_REQUEST = "booking_request"
    GENERAL_QUESTION = "general_question"