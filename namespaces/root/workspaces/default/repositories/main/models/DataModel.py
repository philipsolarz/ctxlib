from __future__ import annotations

from typing import List, Optional

from docarray.base_doc.doc import BaseDoc
from pydantic import AnyUrl, Field


class DataModel(BaseDoc):
    id: Optional[str] = Field(
        None,
        description='The ID of the BaseDoc. This is useful for indexing in vector stores. If not set by user, it will automatically be assigned a random value',
        example='bfd79dcfe9fd55b6941c73353caff4c0',
        title='Id',
    )
    text: Optional[str] = Field(None, title='Text')
    url: Optional[AnyUrl] = Field(None, title='Url')
    embedding: Optional[List[float]] = Field(None, title='Embedding')
    bytes_: Optional[bytes] = Field(None, title='Bytes ')
