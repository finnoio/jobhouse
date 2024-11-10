from datetime import datetime, timedelta
import asyncio
import gzip
import json
import logging
from pathlib import Path
from typing import AsyncIterator, Optional
import aioboto3
import aiopg
import io

from src.common.data import StorageMetadata, RawJobPosting

logger = logging.getLogger(__name__)


class S3RawLayerStorage:
    def __init__(
            self,
            bucket: str,
            metadata_db_path: str,
            endpoint_url: str,  # MinIO endpoint
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str = 'us-east-1'  # MinIO default
    ):
        self.bucket = bucket
        self.metadata_db_path = metadata_db_path
        self._s3_config = {
            'endpoint_url': endpoint_url,
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'region_name': region_name
        }
        self._write_locks = {}  # Key prefix locks for concurrent writes
        self._session = aioboto3.Session()

    async def initialize(self):
        """Initialize storage (create bucket and database)"""
        # Initialize bucket
        async with self._session.client('s3', **self._s3_config) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket)
            except:
                await s3.create_bucket(Bucket=self.bucket)

        # Initialize pg database for metadata
        async with aiopg.connect(self.metadata_db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS posting_metadata (
                    posting_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL, 
                    batch_id TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    etag TEXT NOT NULL,

                    INDEX idx_source_batch (source, batch_id),
                    INDEX idx_created_at (created_at)
                )
            ''')
            await db.commit()

    def _get_s3_key(self, posting: RawJobPosting, batch_id: str) -> str:
        """Generate S3 key for a posting batch"""
        now = datetime.now()
        return f"{posting.source_id}/{now.year}/{now.month:02d}/{now.day:02d}/{batch_id}.jsonl.gz"

    async def store_postings(self, postings: AsyncIterator[RawJobPosting], batch_id: str) -> AsyncIterator[
        StorageMetadata]:
        """Store a batch of postings in S3"""
        buffer = {}  # s3_key -> list of postings
        buffer_size = 1000  # Increased buffer size for S3 (fewer API calls)

        async for posting in postings:
            s3_key = self._get_s3_key(posting, batch_id)

            if s3_key not in buffer:
                buffer[s3_key] = []

            buffer[s3_key].append(posting)

            # Write buffer if it's full
            if len(buffer[s3_key]) >= buffer_size:
                async for metadata in self._write_buffer(s3_key, buffer[s3_key], batch_id):
                    yield metadata
                buffer[s3_key] = []

        # Write remaining buffers
        for s3_key, remaining in buffer.items():
            if remaining:
                async for metadata in self._write_buffer(s3_key, remaining, batch_id):
                    yield metadata

    async def _write_buffer(self, s3_key: str, postings: list[RawJobPosting], batch_id: str) -> AsyncIterator[
        StorageMetadata]:
        """Write a buffer of postings to S3"""
        # Prepare JSONL data
        buffer = io.BytesIO()
        gzip_buffer = gzip.GzipFile(mode='wb', fileobj=buffer)

        metadata_list = []

        for posting in postings:
            record = {
                'posting_id': posting.source_id,
                'raw_content': posting.raw_content,
                'metadata': posting.metadata,
                'timestamp': posting.timestamp.isoformat()
            }
            line = json.dumps(record) + '\n'
            gzip_buffer.write(line.encode())

        gzip_buffer.close()
        compressed_data = buffer.getvalue()

        # Upload to S3
        async with self._session.client('s3', **self._s3_config) as s3:
            response = await s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip'
            )

            # Create metadata records
            for posting in postings:
                metadata = StorageMetadata(
                    posting_id=posting.source_id,
                    source=posting.metadata.get('source', 'unknown'),
                    batch_id=batch_id,
                    s3_key=s3_key,
                    created_at=datetime.now(),
                    size_bytes=len(compressed_data),
                    etag=response['ETag'].strip('"')
                )
                metadata_list.append(metadata)

        # Store metadata
        async with aiosqlite.connect(self.metadata_db_path) as db:
            await db.executemany('''
                INSERT OR REPLACE INTO posting_metadata
                (posting_id, source, batch_id, s3_key, created_at, size_bytes, etag)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', [(m.posting_id, m.source, m.batch_id, m.s3_key, m.created_at, m.size_bytes, m.etag)
                  for m in metadata_list])
            await db.commit()

        for metadata in metadata_list:
            yield metadata

    async def get_posting_metadata(self, posting_id: str) -> Optional[StorageMetadata]:
        """Retrieve metadata for a specific posting"""
        async with aiosqlite.connect(self.metadata_db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                    'SELECT * FROM posting_metadata WHERE posting_id = ?',
                    (posting_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return StorageMetadata(**dict(row))
                return None

    async def implement_retention_policy(self, days: int):
        """Remove objects older than specified days"""
        cutoff = datetime.now() - timedelta(days=days)

        async with aiosqlite.connect(self.metadata_db_path) as db:
            # Get objects to delete
            async with db.execute(
                    'SELECT DISTINCT s3_key FROM posting_metadata WHERE created_at < ?',
                    (cutoff,)
            ) as cursor:
                keys_to_delete = [row[0] for row in await cursor.fetchall()]

            # Delete from S3 in batches
            if keys_to_delete:
                async with self._session.client('s3', **self._s3_config) as s3:
                    # S3 delete_objects accepts max 1000 keys at a time
                    batch_size = 1000
                    for i in range(0, len(keys_to_delete), batch_size):
                        batch = keys_to_delete[i:i + batch_size]
                        await s3.delete_objects(
                            Bucket=self.bucket,
                            Delete={
                                'Objects': [{'Key': key} for key in batch],
                                'Quiet': True
                            }
                        )

            # Remove metadata
            await db.execute(
                'DELETE FROM posting_metadata WHERE created_at < ?',
                (cutoff,)
            )
            await db.commit()


# Example usage:
async def main():
    # Initialize storage
    storage = S3RawLayerStorage(
        bucket="raw-layer",
        metadata_db_path="/data/raw/metadata.db",
        endpoint_url="http://localhost:9000",  # MinIO endpoint
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
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


if __name__ == "__main__":
    asyncio.run(main())