from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# --- Pydantic model for API path and query params ---
class ActivityPathParams(BaseModel):
    userKey: str = "all"
    applicationName: str = "TOKEN"

    def get_path(self) -> str:
        return f"/admin/reports/v1/activity/users/{self.userKey}/applications/{self.applicationName}"


class ActivityQueryParams(BaseModel):
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


# --- Pydantic model for parsing and formatting token event responses ---
class EventParameter(BaseModel):
    name: str
    value: Optional[str] = None
    intValue: Optional[int] = None


class Event(BaseModel):
    name: str
    type: str
    parameters: List[EventParameter]


class NetworkInfo(BaseModel):
    ipAsn: Optional[List[int]] = None
    regionCode: Optional[str] = None
    subdivisionCode: Optional[str] = None


class RawTokenActivity(BaseModel):
    id: dict
    actor: dict
    events: List[Event]
    ipAddress: Optional[str] = None
    networkInfo: Optional[NetworkInfo] = None

    def to_flat_dict(self) -> dict:
        ts_raw = self.id.get("time")
        timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

        result = {
            "timestamp": timestamp,
            "user": self.actor.get("email"),
            "profile_id": self.actor.get("profileId"),
            "unique_id": self.id.get("uniqueQualifier"),
            # network info fields
            "ip": self.ipAddress,
            "asn": None,
            "region_code": None,
            "subdivision_code": None,
            "event_type": None,
            "event_name": None,
            "method_name": None,
            "num_bytes": 0,
            "api_name": None,
            "client_id": None,
            "app_name": None,
            "client_type": None,
            "product_bucket": None,
        }

        if self.networkInfo:
            if self.networkInfo.ipAsn:
                result["asn"] = self.networkInfo.ipAsn[0]
            result["region_code"] = self.networkInfo.regionCode
            result["subdivision_code"] = self.networkInfo.subdivisionCode

        if self.events:
            event = self.events[0]
            result["event_type"] = event.type
            result["event_name"] = event.name

            for param in event.parameters:
                if param.name == "method_name":
                    result["method_name"] = param.value
                elif param.name == "num_response_bytes":
                    result["num_bytes"] = param.intValue or 0
                elif param.name == "api_name":
                    result["api_name"] = param.value
                elif param.name == "client_id":
                    result["client_id"] = param.value
                elif param.name == "app_name":
                    result["app_name"] = param.value
                elif param.name == "client_type":
                    result["client_type"] = param.value
                elif param.name == "product_bucket":
                    result["product_bucket"] = param.value

        return result
