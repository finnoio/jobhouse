import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional, Set, Dict
import asyncio
import aiohttp
import logging
from collections import deque

from src.common.data import RawJobPosting

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logging.getLogger('aiohttp.client').setLevel(logging.DEBUG)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def on_request_start(session, trace_config_ctx, params):
    logger.info(f"Starting request {params.method} {params.url}")
    trace_config_ctx.start = asyncio.get_event_loop().time()

async def on_request_end(session, trace_config_ctx, params):
    elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
    logger.info(f"Request {params.method} {params.url} completed in {elapsed:.3f} seconds")
    logger.info(f"Status: {params.response.status}")
    logger.info(f"Headers: {params.response.headers}")

async def on_request_exception(session, trace_config_ctx, params):
    logger.error(f"Request error: {params.exception}")


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

    def acquire(self) -> None:
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
                time.sleep(sleep_time)

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

    def _fetch_single_page(self, page: int, search_text: str) -> Dict:
        """Fetch a single page of vacancy listings."""
        while True:
            self._rate_limiter.acquire()

            params = {
                'text': search_text,
                'page': page,
                'area': 1,  # Moscow
                'per_page': 100,  # Max allowed by hh.ru
            }

            try:
                with self._session.get(
                        f"{self.base_url}/vacancies",
                        params=params
                ) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limit exceeded on page {page}, backing off...")
                        time.sleep(5)
                        continue

                    return response.json()
            except Exception as e:
                logger.error(f"Unexpected error while fetching page {page}: {str(e)}")
                raise

    async def fetch_vacancies(self, search_text: str) -> list[RawJobPosting]:
        """Fetch vacancies from /vacancies?text=search_text endpoint"""
        first_page = self._fetch_single_page(0, search_text)
        if not first_page or not first_page.get('items'):
            return list()

        total_pages = first_page['pages']
        logger.info(f"Total pages to fetch: {total_pages}")

        # Create tasks for all pages
        tasks = [
            self._fetch_single_page(page, search_text)
            for page in range(total_pages)
        ]

        # Fetch all pages in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        final_result = []
        for page_num, result in enumerate(results):
            if isinstance(result, Exception):
                err_message = f"Failed to fetch page {page_num}: {result}"
                logger.error(err_message)
                raise aiohttp.ClientError(err_message)
            elif result and result.get('items'):
                page = [
                    RawJobPosting(
                        posting_id=str(item['id']),
                        raw_content=item,
                        metadata={
                            'search_text': search_text
                        },
                        source='HH',
                        extracted_at=datetime.now()
                    ) for item in result['items']
                ]

                final_result.extend(page)

        return final_result

    # async def _fetch_single_vacancy(self, vacancy_id: str, search_text: str) -> RawJobPosting:
    #     """Fetch details for a single vacancy ID."""
    #     while True:
    #         await self._rate_limiter.acquire()
    #
    #         try:
    #             async with self._session.get(
    #                     f"{self.base_url}/vacancies/{vacancy_id}"
    #             ) as response:
    #                 if response.status == 429:
    #                     logger.warning(f"Rate limit exceeded for vacancy {vacancy_id}, backing off...")
    #                     await asyncio.sleep(5)
    #                     continue
    #
    #                 response.raise_for_status()
    #                 detail_data = await response.json()
    #
    #                 return RawJobPosting(
    #                     source_id=vacancy_id,
    #                     raw_content=detail_data,
    #                     metadata={
    #                         'search_text': search_text,
    #                         'source': 'HH',
    #                     },
    #                     timestamp=datetime.now()
    #                 )
    #         except Exception as e:
    #             logger.error(f"Unexpected error while fetching vacancy {vacancy_id}: {str(e)}")
    #             raise
    #
    # async def fetch_postings(self, search_text: str) -> AsyncIterator[RawJobPosting]:
    #     await self._ensure_session()
    #
    #     # Collect all vacancy IDs (now in parallel)
    #     all_vacancy_ids = await self._fetch_all_vacancy_ids(search_text)
    #     logger.info(f"Collected {len(all_vacancy_ids)} vacancy IDs")
    #
    #     # Fetch details for all vacancies concurrently
    #     batch_size = 10
    #     vacancy_ids = list(all_vacancy_ids)
    #
    #     for i in range(0, len(vacancy_ids), batch_size):
    #         batch = vacancy_ids[i:i + batch_size]
    #         tasks = [
    #             self._fetch_single_vacancy(vacancy_id, search_text)
    #             for vacancy_id in batch
    #         ]
    #
    #         results = await asyncio.gather(*tasks, return_exceptions=True)
    #
    #         for result in results:
    #             if isinstance(result, Exception):
    #                 logger.error(f"Failed to fetch vacancy details: {result}")
    #                 continue
    #             yield result
