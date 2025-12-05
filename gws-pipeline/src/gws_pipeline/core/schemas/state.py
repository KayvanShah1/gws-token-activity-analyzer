from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class FetcherState(BaseModel):
    """State captured after the fetcher completes."""

    last_run: datetime
    last_event_time: Optional[datetime] = None
    run_id: Optional[str] = None
    snapshot_path: Optional[str] = None


class ProcessorState(BaseModel):
    """State captured after the processor completes."""

    last_processed_event_time: Optional[datetime] = None
    last_processed_run_id: Optional[str] = None
    status: Optional[str] = None


class DuckDBLoaderState(BaseModel):
    """State captured after the DuckDB loader completes."""

    last_loaded_event_time: Optional[datetime] = None
    last_loaded_run_id: Optional[str] = None
    status: Optional[str] = None


class AppState(BaseModel):
    """Combined per-application state for fetcher + processor."""

    version: int = 1
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    fetcher: FetcherState
    processor: ProcessorState = Field(default_factory=ProcessorState)
    duckdb_loader: DuckDBLoaderState = Field(default_factory=DuckDBLoaderState)

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}
