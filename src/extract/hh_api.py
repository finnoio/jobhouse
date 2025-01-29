import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional, Set, Dict
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
            access_token = os.getenv("HH_ACCESS_TOKEN")  # TODO: Move to config
            headers = {"Authorization": f"Bearer {access_token}"}
            self._session = aiohttp.ClientSession(headers=headers)

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
        return RateLimiter(calls=1, period=1)

    async def _fetch_single_page(self, page: int, search_text: str) -> Dict:
        """Fetch a single page of vacancy listings."""
        while True:
            await self._rate_limiter.acquire()

            params = {
                'text': search_text,
                'page': page,
                'area': 1,  # Moscow
                'per_page': 100,  # Max allowed by hh.ru
            }

            try:
                async with self._session.get(
                        f"{self.base_url}/vacancies",
                        params=params
                ) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit exceeded on page {page}, backing off...")
                        await asyncio.sleep(5)
                        continue

                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                logger.error(f"Unexpected error while fetching page {page}: {str(e)}")
                raise

    async def _fetch_all_vacancy_ids(self, search_text: str) -> Set[str]:
        """First phase: Collect all vacancy IDs by fetching all pages in parallel."""
        first_page = await self._fetch_single_page(0, search_text)
        if not first_page or not first_page.get('items'):
            return set()

        total_pages = first_page['pages']
        logger.info(f"Total pages to fetch: {total_pages}")

        # Create tasks for all pages (including page 0 again as it's simpler)
        tasks = [
            self._fetch_single_page(page, search_text)
            for page in range(total_pages)
        ]

        # Fetch all pages in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and collect IDs
        all_ids = set()
        for page_num, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch page {page_num}: {result}")
                continue
            if result and result.get('items'):
                page_ids = {str(item['id']) for item in result['items']}
                all_ids.update(page_ids)

        return all_ids

    async def _fetch_single_vacancy(self, vacancy_id: str, search_text: str) -> RawJobPosting:
        """Fetch details for a single vacancy ID."""
        while True:
            await self._rate_limiter.acquire()

            try:
                async with self._session.get(
                        f"{self.base_url}/vacancies/{vacancy_id}"
                ) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit exceeded for vacancy {vacancy_id}, backing off...")
                        await asyncio.sleep(5)
                        continue

                    response.raise_for_status()
                    detail_data = await response.json()

                    return RawJobPosting(
                        source_id=vacancy_id,
                        raw_content=detail_data,
                        metadata={
                            'search_text': search_text,
                            'found_at': datetime.now().isoformat(),
                            'source': 'HH',
                        },
                        timestamp=datetime.now()
                    )
            except Exception as e:
                logger.error(f"Unexpected error while fetching vacancy {vacancy_id}: {str(e)}")
                raise

    async def fetch_postings(self, search_text: str) -> AsyncIterator[RawJobPosting]:
        await self._ensure_session()

        # Collect all vacancy IDs (now in parallel)
        all_vacancy_ids = await self._fetch_all_vacancy_ids(search_text)
        logger.info(f"Collected {len(all_vacancy_ids)} vacancy IDs")

        # Fetch details for all vacancies concurrently
        batch_size = 10
        vacancy_ids = list(all_vacancy_ids)

        for i in range(0, len(vacancy_ids), batch_size):
            batch = vacancy_ids[i:i + batch_size]
            tasks = [
                self._fetch_single_vacancy(vacancy_id, search_text)
                for vacancy_id in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch vacancy details: {result}")
                    continue
                yield result
