from datetime import datetime

from pydantic import BaseModel, Field, Json


class RawJobPosting(BaseModel):
    posting_id: str
    raw_content: dict
    metadata: dict
    source: str
    extracted_at: datetime


class StorageMetadata(BaseModel):
    source: str
    batch_id: str
    s3_key: str
    created_at: datetime
    etag: str