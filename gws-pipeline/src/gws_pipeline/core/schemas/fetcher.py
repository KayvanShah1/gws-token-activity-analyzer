from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class Application(str, Enum):
    TOKEN = "TOKEN"
    LOGIN = "login"
    SAML = "saml"
    ADMIN = "admin"
    DRIVE = "drive"


# --- Pydantic models for API path and query params ---
class ActivityPathParams(BaseModel):
    """Holds path parameters for Google Workspace Reports API."""

    userKey: str = "all"
    applicationName: Application = Application.TOKEN

    def get_relative_path(self) -> str:
        return f"/admin/reports/v1/activity/users/{self.userKey}/applications/{self.applicationName.value}"

    def get_full_endpoint(self, base_url: str) -> str:
        """Returns full URL including base_url."""
        return f"{base_url}{self.get_relative_path()}"

    @classmethod
    def for_application(cls, application: Application, user_key: str = "all") -> "ActivityPathParams":
        """Convenience constructor for a given application and user key."""
        return cls(userKey=user_key, applicationName=application)


class ActivityQueryParams(BaseModel):
    """Query parameters for the Google Workspace Reports API endpoints."""

    startTime: datetime
    endTime: Optional[datetime] = None
    maxResults: int = 1000
    pageToken: Optional[str] = None

    def to_dict(self):
        return {
            k: v.isoformat() if isinstance(v, datetime) else v for k, v in self.model_dump(exclude_none=True).items()
        }


# --- Pydantic models for run snapshots ---
class WindowRange(BaseModel):
    """Metadata for a single time window processed in a run."""

    idx: int = Field(..., description="Window index within the run (0-based)")
    start: datetime = Field(..., description="Window start time (API query start)")
    end: datetime = Field(..., description="Window end time (API query end)")


class RunSnapshot(BaseModel):
    """Lightweight per-run snapshot of what was fetched."""

    start: datetime = Field(..., description="Window start time (API query start)")
    end: datetime = Field(..., description="Window end time (API query end)")
    run_id: str = Field(..., description="Logical run identifier, e.g. run_start_time formatted")

    # Global event-time coverage for this run
    earliest_event_time: Optional[datetime] = Field(
        None, description="Earliest event_time seen across all windows in this run"
    )
    latest_event_time: Optional[datetime] = Field(
        None, description="Latest event_time seen across all windows in this run"
    )

    # Aggregate stats
    num_windows: int = Field(..., description="Number of windows processed")
    num_events: int = Field(..., description="Total events fetched in this run")

    # Per-window ranges (for your 2-day window / overlap analysis)
    windows: List[WindowRange] = Field(
        default_factory=list,
        description="List of window ranges and optional per-window stats",
    )

    model_config = {
        "json_encoders": {
            datetime: lambda dt: dt.isoformat(),
        }
    }
