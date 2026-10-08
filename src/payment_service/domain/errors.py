class DomainError(Exception):
    """Base class for service errors."""


class PermanentError(DomainError):
    """Processing can never succeed for this message: dead-letter it without retries."""


class TransientError(DomainError):
    """Processing may succeed later: retry with backoff."""


class PaymentNotFoundError(PermanentError):
    def __init__(self, payment_id: object) -> None:
        super().__init__(f"payment {payment_id} not found")
        self.payment_id = payment_id


class IdempotencyKeyReusedError(DomainError):
    """Same Idempotency-Key was used with a different request body."""


class WebhookDeliveryError(TransientError):
    """Webhook endpoint failed (non-2xx, timeout, connection error)."""
