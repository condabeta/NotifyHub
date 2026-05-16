from enum import StrEnum


class Channel(StrEnum):
    SMS = "sms"
    EMAIL = "email"


class Priority(StrEnum):
    TRANSACTIONAL = "transactional"
    MARKETING = "marketing"


class NotificationStatus(StrEnum):
    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"


PRIORITY_RANK: dict[Priority, int] = {
    Priority.TRANSACTIONAL: 10,
    Priority.MARKETING: 1,
}
