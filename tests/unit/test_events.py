from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payment_service.domain.events import PaymentCreatedEvent


def test_event_round_trip() -> None:
    event = PaymentCreatedEvent(event_id=uuid4(), payment_id=uuid4(), occurred_at=datetime.now(UTC))
    restored = PaymentCreatedEvent.model_validate_json(event.model_dump_json())
    assert restored == event
    assert restored.occurred_at.tzinfo is not None


def test_event_id_and_occurred_at_are_required_on_decode() -> None:
    with pytest.raises(ValidationError):
        PaymentCreatedEvent.model_validate({"payment_id": str(uuid4())})
