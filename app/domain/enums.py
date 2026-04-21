from enum import Enum


class IntentType(str, Enum):
    PRICE = "price"
    CHANNELS = "channels"
    SERVICE_DESCRIPTION = "service_description"
    CONSULTATION_INTEREST = "consultation_interest"
    BOOKING_REQUEST = "booking_request"
    GENERAL_QUESTION = "general_question"