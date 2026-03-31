"""Base class for ad platform integrations."""
from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class AdPlatformClient(ABC):
    """Abstract base for platform-specific API clients."""

    @abstractmethod
    async def fetch_campaign_metrics(
        self,
        account_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Return a list of metric rows for all campaigns in the given date range."""
        ...

    @abstractmethod
    async def fetch_budget_pacing(self, account_id: str) -> list[dict[str, Any]]:
        """Return current budget pacing data for all active campaigns."""
        ...
