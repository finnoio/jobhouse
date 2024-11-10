import asyncio

from src.extract.async_client import HHAsyncClient


async def main():
    config = {
        'user_agent': 'curl/7.64.1'
    }

    source = HHAsyncClient(config)
    try:
        async for posting in source.fetch_postings("data engineer"):
            print(f"Found posting: {posting.source_id}")
            print(f"Found posting: {posting.raw_content}")
    finally:
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
