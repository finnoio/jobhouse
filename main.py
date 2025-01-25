import asyncio
import json
from pathlib import Path
import logging
from datetime import datetime

from src.extract.hh_api import HHAsyncClient
from src.storage.raw_layer import S3RawLayerStorage

logger = logging.getLogger(__name__)


async def main():
    # Initialize storage
    storage = S3RawLayerStorage(
        bucket="raw-layer",
        metadata_db_conn_str="postgresql://postgres:postgres@localhost:5432/jobs_meta",
        endpoint_url="http://localhost:9000",  # MinIO endpoint
        aws_access_key_id="minio_access_key",
        aws_secret_access_key="minio_secret_key"
    )
    await storage.initialize()

    # Initialize source
    config = {
        'user_agent': 'curl/7.64.1'
    }

    source = HHAsyncClient(config)
    try:
        # Generate batch ID
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Store postings
        async for metadata in storage.store_postings(
                source.fetch_postings("python developer"),
                batch_id
        ):
            logger.info(f"Stored posting {metadata.posting_id} in s3://{storage.bucket}/{metadata.s3_key}")

        # Implement retention policy
        await storage.implement_retention_policy(days=7)
    finally:
        await source.close()

# async def main():
#     config = {
#         'user_agent': 'PersonalAnalyticsApp/1.0'
#     }
#
#     source = HHAsyncClient(config)
#     all_postings = []
#     await source._ensure_session()
#     try:
#         async for posting in source.fetch_postings("data engineer"):
#             all_postings.append(posting.raw_content)
#     finally:
#         await source.close()
#
#     output_file = "postings.json"
#     with Path(output_file).open("w", encoding="utf-8") as f:
#         json.dump(all_postings, f, ensure_ascii=False, indent=4)
#         print(f"Saved all postings to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
