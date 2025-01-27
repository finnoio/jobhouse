from datetime import datetime, timedelta
import gzip
import json
import logging
from typing import AsyncIterator, Optional
import aioboto3
import aiopg
import io

from src.jobhouse.common.data import StorageMetadata, RawJobPosting

logger = logging.getLogger(__name__)


class S3RawLayerStorage:
    def __init__(
            self,
            bucket: str,
            metadata_db_conn_str: str,
            endpoint_url: str,  # MinIO endpoint
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str = 'us-east-1'  # MinIO default
    ):
        self.bucket = bucket
        self.metadata_db_conn_str = metadata_db_conn_str
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
        async with aiopg.create_pool(self.metadata_db_conn_str) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''CREATE TABLE IF NOT EXISTS posting_metadata (
                            posting_id TEXT PRIMARY KEY,
                            source TEXT NOT NULL, 
                            batch_id TEXT NOT NULL,
                            s3_key TEXT NOT NULL,
                            created_at TIMESTAMP NOT NULL,
                            size_bytes INTEGER NOT NULL,
                            etag TEXT NOT NULL
                        )''')

                    await cur.execute('''
                        CREATE INDEX IF NOT EXISTS idx_posting_metadata_posting_id ON posting_metadata (posting_id);
                    ''')
                    await cur.execute('''
                        CREATE INDEX IF NOT EXISTS idx_posting_metadata_source_batch_id ON posting_metadata (source, batch_id);
                    ''')

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
        async with aiopg.connect(self.metadata_db_conn_str) as db:
            async with db.cursor() as cur:
                await cur.executemany('''
                    INSERT OR REPLACE INTO posting_metadata
                    (posting_id, source, batch_id, s3_key, created_at, size_bytes, etag)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', [(m.posting_id, m.source, m.batch_id, m.s3_key, m.created_at, m.size_bytes, m.etag) for m in metadata_list])

        for metadata in metadata_list:
            yield metadata

    async def get_posting_metadata(self, posting_id: str) -> Optional[StorageMetadata]:
        """Retrieve metadata for a specific posting"""

        async with aiopg.connect(self.metadata_db_conn_str) as db:
            async with db.cursor() as cur:
                await cur.execute(
                        'SELECT * FROM posting_metadata WHERE posting_id = ?',
                        (posting_id,)
                )
                row = await cur.fetchone()
                if row:
                    return StorageMetadata(**dict(row))
                else:
                    return None

    async def implement_retention_policy(self, days: int):
        """Remove objects older than specified days"""
        cutoff = datetime.now() - timedelta(days=days)
        async with aiopg.connect(self.metadata_db_conn_str) as db:
            async with db.cursor() as cur:
                # Get objects to delete
                await cur.execute(
                    'SELECT DISTINCT s3_key FROM posting_metadata WHERE created_at < %s',
                    (cutoff,)
                )
                keys_to_delete = [row[0] for row in await cur.fetchall()]

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

            async with db.cursor() as cur:
                await cur.execute(
                    'DELETE FROM posting_metadata WHERE created_at < %s',
                    (cutoff,)
                )
