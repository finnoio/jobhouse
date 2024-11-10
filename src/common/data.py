from dataclasses import dataclass
from datetime import datetime

@dataclass
class RawJobPosting:
    source_id: str
    raw_content: str
    metadata: dict
    timestamp: datetime


@dataclass
class StorageMetadata:
    posting_id: str
    source: str
    batch_id: str
    s3_key: str
    created_at: datetime
    size_bytes: int
    etag: str