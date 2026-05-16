from __future__ import annotations

from app.config import Settings
from app.core.enums import Channel
from app.providers.base import Provider
from app.providers.mock import MockProvider


class ProviderRegistry:
    """Looks up the provider implementation responsible for a given channel.

    Today only mock implementations exist; real providers would be plugged in
    here without touching the worker or service layer.
    """

    def __init__(self, providers: dict[Channel, Provider]) -> None:
        self._providers = providers

    @classmethod
    def from_settings(cls, settings: Settings) -> "ProviderRegistry":
        return cls(
            {
                Channel.SMS: MockProvider.for_channel(Channel.SMS, settings),
                Channel.EMAIL: MockProvider.for_channel(Channel.EMAIL, settings),
            }
        )

    def get(self, channel: Channel) -> Provider:
        return self._providers[channel]

    def set(self, channel: Channel, provider: Provider) -> None:
        self._providers[channel] = provider
