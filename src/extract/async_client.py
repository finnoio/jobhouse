from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional
import asyncio
import aiohttp
import logging
from collections import deque

from src.common.data import RawJobPosting

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple time-window rate limiter"""

    def __init__(self, calls: int, period: float):
        """
        Args:
            calls: Number of calls allowed
            period: Time period in seconds
        """
        self.calls = calls
        self.period = period
        self.timestamps = deque()

    async def acquire(self) -> None:
        """Wait until a call can be made without violating the rate limit"""
        now = datetime.now()

        # Remove timestamps outside the current window
        while self.timestamps and now - self.timestamps[0] > timedelta(seconds=self.period):
            self.timestamps.popleft()

        if len(self.timestamps) >= self.calls:
            # Wait until the oldest timestamp expires
            sleep_time = (self.timestamps[0] + timedelta(seconds=self.period) - now).total_seconds()
            if sleep_time > 0:
                logger.debug(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)

        self.timestamps.append(now)


class AsyncHttpClientBase(ABC):
    def __init__(self, config: dict):
        self.config = config
        self._rate_limiter = self._init_rate_limiter()
        self._session: Optional[aiohttp.ClientSession] = None

    @abstractmethod
    def _init_rate_limiter(self) -> RateLimiter:
        """Initialize source-specific rate limiter"""
        pass

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self._session is None:
            self._session = aiohttp.ClientSession()

    async def close(self):
        """Close the aiohttp session"""
        if self._session is not None:
            await self._session.close()
            self._session = None


class HHAsyncClient(AsyncHttpClientBase):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = "https://api.hh.ru"

    def _init_rate_limiter(self) -> RateLimiter:
        # HH.ru allows 7 requests per second (https://github.com/hhru/api/issues/74#issuecomment-902696296)
        return RateLimiter(calls=7, period=1)

    async def fetch_postings(self, search_text: str, page: int = 0) -> AsyncIterator[RawJobPosting]:
        await self._ensure_session()

        while True:
            await self._rate_limiter.acquire()

            # API request params
            params = {
                'text': search_text,
                'page': page,
                'area': 1,  # Moscow
                'per_page': 100,  # Max allowed by hh.ru
            }

            try:
                async with self._session.get(f"{self.base_url}/vacancies", params=params) as response:
                    if response.status == 429:  # Too Many Requests
                        logger.warning("Rate limit exceeded, backing off...")
                        await asyncio.sleep(5)  # Simple backoff
                        continue

                    response.raise_for_status()
                    data = await response.json()

                    # No more results
                    if not data['items']:
                        break

                    # Yield each posting
                    for item in data['items']:
                        yield RawJobPosting(
                            source_id=str(item['id']),
                            raw_content=str(item),
                            metadata={
                                'search_text': search_text,
                                'page': page,
                                'found_at': datetime.now().isoformat()
                            },
                            timestamp=datetime.now()
                        )

                    # Check if we have more pages
                    if page >= data['pages'] - 1:
                        break

                    page += 1

            except aiohttp.ClientError as e:
                logger.error(f"Error fetching page {page}: {e}")
                raise
