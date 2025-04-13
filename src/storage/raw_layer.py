from datetime import datetime, timedelta
import gzip
import json
import logging
from typing import AsyncIterator, Optional
# import aioboto3
import io

from src.common.data import StorageMetadata, RawJobPosting

logger = logging.getLogger(__name__)


class S3RawLayerStorage:
    def __init__(
            self,
            bucket: str,
            s3_session,
            # region_name: str = 'us-east-1'  # MinIO default
    ):
        self.bucket = bucket
        self._session = s3_session


    def _get_s3_key(self, posting: RawJobPosting, batch_id: str) -> str:
        """Generate S3 key for a posting batch"""
        extracted_dt = posting.extracted_at
        return f"{posting.source}/{extracted_dt.year}/{extracted_dt.month:02d}/{extracted_dt.day:02d}/{extracted_dt.hour:02d}/{batch_id}.jsonl.gz"

    def store_postings(self, postings: list[RawJobPosting], batch_id: str) -> list[StorageMetadata]:
        """Store a batch of postings in S3"""
        s3_key = self._get_s3_key(postings[0], batch_id)
        buffer = io.BytesIO()
        gzip_buffer = gzip.GzipFile(mode='wb', fileobj=buffer)

        metadata_list = []

        for posting in postings:
            record = posting.model_dump()
            line = json.dumps(record) + '\n'
            gzip_buffer.write(line.encode())

        gzip_buffer.close()
        compressed_data = buffer.getvalue()

        # Upload to S3
        with self._session.client('s3') as s3:
            response = s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip'
            )

            # Create metadata records
            for posting in postings:
                metadata = StorageMetadata(
                    source=posting.metadata['source'],
                    batch_id=batch_id,
                    s3_key=s3_key,
                    created_at=datetime.now(),
                    etag=response['ETag'].strip('"')
                )
                metadata_list.append(metadata)

        return metadata_list
