from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple, Union

from pydantic import BaseModel, Field


# --- Common audit metadata models (shared across app types) ---
class ActivityId(BaseModel):
    time: str
    unique_qualifier: str = Field(..., alias="uniqueQualifier")
    application_name: str = Field(..., alias="applicationName")
    customer_id: Optional[str] = Field(default=None, alias="customerId")


class ActorApplicationInfo(BaseModel):
    oauth_client_id: Optional[str] = Field(default=None, alias="oauthClientId")
    application_name: Optional[str] = Field(default=None, alias="applicationName")
    impersonation: Optional[bool] = None


class ActorInfo(BaseModel):
    caller_type: Optional[str] = Field(default=None, alias="callerType")
    email: Optional[str] = None
    profile_id: Optional[str] = Field(default=None, alias="profileId")
    application_info: Optional[ActorApplicationInfo] = Field(default=None, alias="applicationInfo")


class AppliedLabelReason(BaseModel):
    reason_type: Optional[str] = Field(default=None, alias="reasonType")


class SelectionValue(BaseModel):
    id: Optional[str] = None
    display_name: Optional[str] = Field(default=None, alias="displayName")
    badged: Optional[bool] = None


class AppliedLabelFieldValue(BaseModel):
    id: Optional[str] = None
    display_name: Optional[str] = Field(default=None, alias="displayName")
    type: Optional[str] = None
    selection_value: Optional[SelectionValue] = Field(default=None, alias="selectionValue")
    reason: Optional[AppliedLabelReason] = None


class AppliedLabel(BaseModel):
    id: str
    title: Optional[str] = None
    reason: Optional[AppliedLabelReason] = None
    field_values: Optional[List[AppliedLabelFieldValue]] = Field(default=None, alias="fieldValues")


class ResourceDetail(BaseModel):
    id: str
    title: Optional[str] = None
    type: Optional[str] = None
    relation: Optional[str] = None
    applied_labels: Optional[List[AppliedLabel]] = Field(default=None, alias="appliedLabels")


# --- Common event models ---
class EventParameter(BaseModel):
    """Parameter for a single event in audit activity."""

    name: str
    value: Optional[str] = None
    bool_value: Optional[bool] = Field(default=None, alias="boolValue")
    int_value: Optional[int] = Field(default=None, alias="intValue")
    multi_value: Optional[List[str]] = Field(default=None, alias="multiValue")
    multi_message_value: Optional[List[Dict[str, Any]]] = Field(default=None, alias="multiMessageValue")


class Event(BaseModel):
    """Single event in audit activity."""

    name: str
    type: str
    parameters: List[EventParameter] = Field(default_factory=list)
    resource_ids: Optional[List[str]] = Field(default=None, alias="resourceIds")


class NetworkInfo(BaseModel):
    """Network information associated with an audit activity event."""

    ip_asn: Optional[List[int]] = Field(default=None, alias="ipAsn")
    region_code: Optional[str] = Field(default=None, alias="regionCode")
    subdivision_code: Optional[str] = Field(default=None, alias="subdivisionCode")


# --- Base activity (shared helpers) ---
class BaseActivity(BaseModel):
    id: Union[ActivityId, Dict[str, Any]]
    actor: Union[ActorInfo, Dict[str, Any]]
    events: List[Event] = Field(default_factory=list)
    ip_address: Optional[str] = Field(default=None, alias="ipAddress")
    network_info: Optional[NetworkInfo] = Field(default=None, alias="networkInfo")
    resource_details: Optional[List[ResourceDetail]] = Field(default=None, alias="resourceDetails")
    kind: Optional[str] = None
    etag: Optional[str] = None

    model_config = {"json_encoders": {datetime: lambda dt: dt.isoformat()}}

    @staticmethod
    def _get_field(container: Union[BaseModel, Dict[str, Any], None], *keys: str) -> Any:
        """Safely pluck a value from either a dict or a Pydantic model."""
        if container is None:
            return None
        for key in keys:
            if isinstance(container, dict):
                if key in container:
                    return container.get(key)
            else:
                if hasattr(container, key):
                    value = getattr(container, key)
                    if value is not None:
                        return value
        return None

    def _parse_timestamp(self) -> datetime:
        ts_raw = self._get_field(self.id, "time")
        # Example: "2025-11-27T03:11:11.616Z"
        # Normalize "Z" to "+00:00" for fromisoformat.
        return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

    def _first_event(self) -> Optional[Event]:
        return self.events[0] if self.events else None

    @staticmethod
    def _param_value(param: EventParameter) -> Any:
        if param.bool_value is not None:
            return param.bool_value
        if param.int_value is not None:
            return param.int_value
        if param.multi_value is not None:
            return param.multi_value
        if param.multi_message_value is not None:
            return param.multi_message_value
        return param.value

    @staticmethod
    def _param_lookup(event: Optional[Event]) -> Dict[str, EventParameter]:
        if not event:
            return {}
        return {p.name: p for p in event.parameters}

    @staticmethod
    def _params_to_dict(params: Dict[str, EventParameter]) -> Optional[Dict[str, Any]]:
        """Convert params to a plain dict; return None when empty to avoid empty struct columns."""
        if not params:
            return None
        return {k: BaseActivity._param_value(v) for k, v in params.items()}

    def _base_record(self, event: Optional[Event]) -> Dict[str, Any]:
        app_info = self._get_field(self.actor, "application_info", "applicationInfo")
        base: Dict[str, Any] = {
            "timestamp": self._parse_timestamp(),
            "unique_id": self._get_field(self.id, "unique_qualifier", "uniqueQualifier"),
            "application_name": self._get_field(self.id, "application_name", "applicationName"),
            "customer_id": self._get_field(self.id, "customer_id", "customerId"),
            "actor_email": self._get_field(self.actor, "email"),
            "actor_profile_id": self._get_field(self.actor, "profile_id", "profileId"),
            "caller_type": self._get_field(self.actor, "caller_type", "callerType"),
            "oauth_client_id": self._get_field(app_info, "oauth_client_id", "oauthClientId"),
            "oauth_app_name": self._get_field(app_info, "application_name", "applicationName"),
            "impersonation": self._get_field(app_info, "impersonation"),
            "ip": self.ip_address,
            "asn": None,
            "asn_list": None,
            "region_code": None,
            "subdivision_code": None,
            "event_type": event.type if event else None,
            "event_name": event.name if event else None,
            "resource_ids": event.resource_ids if event else None,
            "resource_detail_count": len(self.resource_details) if self.resource_details else 0,
        }

        if self.network_info:
            if self.network_info.ip_asn:
                base["asn"] = self.network_info.ip_asn[0]
                base["asn_list"] = self.network_info.ip_asn
            base["region_code"] = self.network_info.region_code
            base["subdivision_code"] = self.network_info.subdivision_code

        return base

    def to_event_record(self) -> Dict[str, Any]:
        """Generic flattening for audit events; override for app-specific enrichment."""
        event = self._first_event()
        base = self._base_record(event)
        params = self._param_lookup(event)
        base["parameters"] = self._params_to_dict(params)
        return base


# --- Token activity (scope-heavy) ---
class RawTokenActivity(BaseActivity):
    """
    Raw token activity event as returned by the Google Workspace Reports API.

    This model provides:
    - to_event_record(): one flat row per event (for the main events table)
    - iter_scope_records(): one row per scope (for the scopes detail table)
    """

    # Map product_bucket → coarse service
    BUCKET_SERVICE_MAP: ClassVar[Dict[str, str]] = {
        "GSUITE_ADMIN": "admin",
        "DRIVE": "drive",
        "GMAIL": "gmail",
        "APPS_SCRIPT_API": "apps_script",
        "APPS_SCRIPT_RUNTIME": "apps_script",
        "CALENDAR": "calendar",
        "IDENTITY": "identity",
        # "OTHER" intentionally omitted → fallback to scope-based logic
    }

    # Map scope "family" → coarse service (for when bucket is missing/OTHER)
    SCOPE_FAMILY_SERVICE_MAP: ClassVar[Dict[str, str]] = {
        # Google Workspace / Admin
        "admin": "admin",
        "apps": "admin",
        "cloud-identity": "admin",
        "cloudplatformprojects": "cloud_platform",
        # Product APIs
        "drive": "drive",
        "drive.addons": "drive",
        "datastudio": "datastudio",
        "spreadsheets": "sheets",
        "gmail": "gmail",
        "script": "apps_script",
        "sqlservice": "cloud_sql",
        "workspace": "workspace",  # workspace.linkpreview, etc.
        "calendar": "calendar",
        "calendar.addons": "calendar",
        # userinfo / OAuth
        "userinfo": "identity",
    }

    # ------------------------------------------------------------------ #
    # Helpers to build analytics-friendly records
    # ------------------------------------------------------------------ #

    @classmethod
    def _normalize_non_google_scope(cls, scope_name: str) -> str:
        """Handle generic / non-googleapis scopes."""
        simple = (scope_name or "").lower()

        if simple == "openid":
            return "identity"
        if simple in {
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.google.com/accounts/oauthlogin",
        }:
            return "identity"

        return "other"

    @classmethod
    def _extract_scope_family(cls, scope_name: str) -> Tuple[str, str]:
        """
        Extract (family, suffix) from scope_name.

        Examples:
          https://www.googleapis.com/auth/drive.readonly
            -> ('drive', 'readonly')
          https://www.googleapis.com/auth/admin.directory.user.readonly
            -> ('admin', 'directory.user.readonly')
          https://www.googleapis.com/auth/calendar.addons.current.event.read
            -> ('calendar.addons', 'current.event.read')
        """
        prefix = "https://www.googleapis.com/auth/"
        if not scope_name.startswith(prefix):
            return "", ""

        tail = scope_name[len(prefix) :]  # e.g. 'drive.readonly', 'admin.directory.user.readonly', ...

        parts = tail.split(".")
        # Special-case multi-word families like 'calendar.addons'
        if len(parts) >= 2 and parts[0] == "calendar" and parts[1] == "addons":
            family = "calendar.addons"
            suffix = ".".join(parts[2:]) if len(parts) > 2 else ""
        else:
            family = parts[0]
            suffix = ".".join(parts[1:]) if len(parts) > 1 else ""

        return family, suffix

    @classmethod
    def _derive_service(cls, scope_name: str, product_bucket: Optional[str]) -> str:
        """
        Coarse service classification:

          1. Use product_bucket if present and mapped
          2. Else classify by scope family
          3. Else fall back to generic non-google handler
        """
        # 1) Bucket-based classification
        if product_bucket:
            svc = cls.BUCKET_SERVICE_MAP.get(product_bucket)
            if svc:
                return svc

        # 2) Scope-name-based classification
        prefix = "https://www.googleapis.com/auth/"
        if scope_name and scope_name.startswith(prefix):
            family, _ = cls._extract_scope_family(scope_name)
            if family:
                svc = cls.SCOPE_FAMILY_SERVICE_MAP.get(family)
                if svc:
                    return svc

        # 3) Generic / non-google scopes
        return cls._normalize_non_google_scope(scope_name or "")

    # ------------------------------------------------------------------ #
    # Public API: token events
    # ------------------------------------------------------------------ #

    def to_event_record(self) -> Dict[str, Any]:
        """
        Return a single flat dict representing the event-level row.
        """
        event = self._first_event()
        params = self._param_lookup(event)
        record: Dict[str, Any] = {
            **self._base_record(event),
            "method_name": None,
            "num_bytes": 0,
            "api_name": None,
            "client_id": None,
            "app_name": None,
            "client_type": None,
            # aggregate scope info
            "scope_count": 0,
            "product_buckets": None,  # list[str] or null
            "has_drive_scope": False,
            "has_gmail_scope": False,
            "has_admin_scope": False,
        }

        # Event parameters
        if params:
            if "method_name" in params:
                record["method_name"] = params["method_name"].value
            if "num_response_bytes" in params:
                record["num_bytes"] = params["num_response_bytes"].int_value or 0
            if "api_name" in params:
                record["api_name"] = params["api_name"].value
            if "client_id" in params:
                record["client_id"] = params["client_id"].value
            if "app_name" in params:
                record["app_name"] = params["app_name"].value
            if "client_type" in params:
                record["client_type"] = params["client_type"].value

        # Aggregate scopes and product buckets
        scopes = list(self.iter_scope_records())
        if scopes:
            record["scope_count"] = len(scopes)
            buckets = {s["product_bucket"] for s in scopes if s["product_bucket"]}
            if buckets:
                record["product_buckets"] = sorted(buckets)
                record["has_drive_scope"] = "DRIVE" in buckets
                record["has_gmail_scope"] = "GMAIL" in buckets
                record["has_admin_scope"] = "GSUITE_ADMIN" in buckets

        return record

    def iter_scope_records(self) -> Iterable[Dict[str, Any]]:
        """
        Yield per-scope records for this event.

        Each yielded dict is one row for the scopes detail table.
        """
        event = self._first_event()
        if not event:
            return

        unique_id = self._get_field(self.id, "unique_qualifier", "uniqueQualifier")
        timestamp = self._parse_timestamp()

        # Preferred source: scope_data (with product_bucket)
        had_scope_data = False
        for msg in self._iter_scope_data_messages(event):
            had_scope_data = True
            scope_name: Optional[str] = None
            product_bucket: Optional[str] = None

            for p in msg.get("parameter", []):
                pname = p.get("name")
                if pname == "scope_name":
                    scope_name = p.get("value")
                elif pname == "product_bucket":
                    mv = p.get("multiValue") or []
                    product_bucket = mv[0] if mv else None

            if scope_name:
                family, _ = self._extract_scope_family(scope_name)
                service = self._derive_service(scope_name, product_bucket)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "scope_family": family or None,
                    "product_bucket": product_bucket,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }

        # Fallback: plain scope list if scope_data is missing
        if not had_scope_data:
            for scope_name in self._iter_scope_list_values(event):
                family, _ = self._extract_scope_family(scope_name)
                service = self._derive_service(scope_name, None)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "scope_family": family or None,
                    "product_bucket": None,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }

    # --- scope helpers ---
    def _iter_scope_data_messages(self, event: Event) -> Iterable[Dict[str, Any]]:
        """Yield raw messages from the 'scope_data' parameter, if present."""
        for param in event.parameters:
            if param.name == "scope_data" and param.multi_message_value:
                for msg in param.multi_message_value:
                    yield msg

    def _iter_scope_list_values(self, event: Event) -> Iterable[str]:
        """Yield scope strings from the 'scope' parameter, if present."""
        for param in event.parameters:
            if param.name == "scope" and param.multi_value:
                for scope_name in param.multi_value:
                    yield scope_name


# --- Admin activity ---
class RawAdminActivity(BaseActivity):
    def to_event_record(self) -> Dict[str, Any]:
        event = self._first_event()
        params = self._param_lookup(event)
        record = {**self._base_record(event)}

        # Capture all parameters as a dict for downstream enrichment/JSON logging.
        record["parameters"] = self._params_to_dict(params)
        return record


# --- Login activity ---
class RawLoginActivity(BaseActivity):
    def to_event_record(self) -> Dict[str, Any]:
        event = self._first_event()
        params = self._param_lookup(event)
        record = {**self._base_record(event)}

        record.update(
            {
                "login_type": self._param_value(params["login_type"]) if "login_type" in params else None,
                "login_challenge_methods": (
                    self._param_value(params["login_challenge_method"]) if "login_challenge_method" in params else None
                ),
                "is_suspicious": self._param_value(params["is_suspicious"]) if "is_suspicious" in params else None,
            }
        )

        return record


# --- Drive activity ---
class RawDriveActivity(BaseActivity):
    def _label_stats(self) -> Tuple[int, List[str]]:
        if not self.resource_details:
            return 0, []
        titles: set[str] = set()
        count = 0
        for rd in self.resource_details:
            for label in rd.applied_labels or []:
                count += 1
                if label.title:
                    titles.add(label.title)
        return count, sorted(titles)

    def to_event_record(self) -> Dict[str, Any]:
        event = self._first_event()
        params = self._param_lookup(event)
        record = {**self._base_record(event)}

        label_count, label_titles = self._label_stats()

        record.update(
            {
                "user_query": self._param_value(params["user_query"]) if "user_query" in params else None,
                "parsed_query": self._param_value(params["parsed_query"]) if "parsed_query" in params else None,
                "primary_event": self._param_value(params["primary_event"]) if "primary_event" in params else None,
                "billable": self._param_value(params["billable"]) if "billable" in params else None,
                "originating_app_id": (
                    self._param_value(params["originating_app_id"]) if "originating_app_id" in params else None
                ),
                "actor_is_collaborator_account": (
                    self._param_value(params["actor_is_collaborator_account"])
                    if "actor_is_collaborator_account" in params
                    else None
                ),
                "resource_id_count": len(event.resource_ids) if event and event.resource_ids else 0,
                "applied_label_count": label_count,
                "applied_label_titles": label_titles if label_titles else None,
            }
        )

        return record


# --- SAML activity ---
class RawSamlActivity(BaseActivity):
    def to_event_record(self) -> Dict[str, Any]:
        event = self._first_event()
        params = self._param_lookup(event)
        record = {**self._base_record(event)}

        record.update(
            {
                "orgunit_path": self._param_value(params["orgunit_path"]) if "orgunit_path" in params else None,
                "initiated_by": self._param_value(params["initiated_by"]) if "initiated_by" in params else None,
                "application_name": (
                    self._param_value(params["application_name"]) if "application_name" in params else None
                ),
                "saml_status_code": (
                    self._param_value(params["saml_status_code"]) if "saml_status_code" in params else None
                ),
                "saml_second_level_status_code": (
                    self._param_value(params["saml_second_level_status_code"])
                    if "saml_second_level_status_code" in params
                    else None
                ),
                "saml_failure_type": self._param_value(params["failure_type"]) if "failure_type" in params else None,
                "parameters": self._params_to_dict(params),
            }
        )

        return record


if __name__ == "__main__":
    # Small local smoke test: parse the example token response.
    import json
    from rich.pretty import pprint
    from gws_pipeline.core import settings

    example_input = settings.base_dir / "input" / "example_response_token_2.json"

    records = []
    scope_records = []
    with open(example_input, "r") as f:
        data = json.loads(f.read())["token"]
        for item in data.get("items", [])[:30]:
            event = RawTokenActivity(**item)
            records.append(event.to_event_record())
            for scope_rec in event.iter_scope_records():
                scope_records.append(scope_rec)

    # pprint(scope_records)
    pprint(records)
