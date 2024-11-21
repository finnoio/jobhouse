import asyncio
import json
from pathlib import Path

from src.extract.hh_api import HHAsyncClient


async def main():
    config = {
        'user_agent': 'curl/7.64.1'
    }

    source = HHAsyncClient(config)
    all_postings = []
    await source._ensure_session()
    try:
        async for posting in source.fetch_postings("data engineer"):
            all_postings.append(posting.raw_content)
    finally:
        await source.close()

    output_file = "postings.json"
    with Path(output_file).open("w", encoding="utf-8") as f:
        json.dump(all_postings, f, ensure_ascii=False, indent=4)
        print(f"Saved all postings to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
