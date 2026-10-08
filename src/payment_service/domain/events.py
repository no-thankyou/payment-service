from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    """Thin event: the consumer reads the current payment state from the DB.

    All fields are required on decode; the producer sets ``event_id`` / ``occurred_at`` explicitly.
    """

    event_id: UUID
    payment_id: UUID
    occurred_at: datetime
