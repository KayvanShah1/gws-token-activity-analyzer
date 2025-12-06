from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.schemas.state import AppState, DuckDBLoaderState, FetcherState, ProcessorState

logger = get_logger("StateStore")


def _default_fetcher_state() -> FetcherState:
    """Fallback fetcher state for first run or unreadable file."""
    ts = datetime.now(timezone.utc) - timedelta(hours=settings.DEFAULT_DELTA_HRS)
    return FetcherState(last_run=ts)


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def load_state(state_path: Path) -> AppState:
    """Load state from disk, supporting legacy `{'last_run': ...}` format."""
    if not state_path.exists():
        logger.warning(f"State file not found at {state_path.relative_to(settings.base_dir)}. Using defaults.")
        return AppState(fetcher=_default_fetcher_state())

    try:
        data = json.loads(state_path.read_text())
    except Exception as exc:
        logger.error(f"Failed to read state file {state_path.relative_to(settings.base_dir)}: {exc}. Using defaults.")
        return AppState(fetcher=_default_fetcher_state())

    # Legacy shape: {"last_run": "..."}
    if "fetcher" not in data and "processor" not in data:
        legacy_ts = _parse_datetime(data.get("last_run"))
        fetcher_state = FetcherState(last_run=legacy_ts or _default_fetcher_state().last_run)
        return AppState(fetcher=fetcher_state)

    try:
        return AppState(**data)
    except Exception as exc:
        logger.error(f"Failed to parse state file {state_path.relative_to(settings.base_dir)}: {exc}. Using defaults.")
        return AppState(fetcher=_default_fetcher_state())


def save_state(state_path: Path, state: AppState) -> None:
    """Persist state to disk."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_dict = state.model_copy(update={"updated_at": datetime.now(timezone.utc)}).model_dump(mode="json")
    state_path.write_text(json.dumps(state_dict, indent=2))


def get_fetcher_cursor(state_path: Path) -> datetime:
    """Return last fetch cursor (no overlap applied)."""
    state = load_state(state_path)
    return state.fetcher.last_run


def update_fetcher_state(
    state_path: Path,
    last_run: datetime,
    *,
    last_event_time: Optional[datetime] = None,
    run_id: Optional[str] = None,
    snapshot_path: Optional[Path] = None,
) -> None:
    """Update fetcher portion while preserving processor state."""
    state = load_state(state_path)
    state.fetcher = FetcherState(
        last_run=last_run,
        last_event_time=last_event_time or last_run,
        run_id=run_id,
        snapshot_path=(
            str(snapshot_path.relative_to(settings.base_dir)) if snapshot_path else state.fetcher.snapshot_path
        ),
    )
    save_state(state_path, state)


def update_processor_state(
    state_path: Path,
    last_processed_event_time: datetime,
    *,
    run_id: Optional[str] = None,
    status: Optional[str] = "success",
) -> None:
    """Update processor portion while preserving fetcher state."""
    state = load_state(state_path)
    state.processor = ProcessorState(
        last_processed_event_time=last_processed_event_time,
        last_processed_run_id=run_id,
        status=status,
    )
    save_state(state_path, state)


def update_duckdb_loader_state(
    state_path: Path,
    last_loaded_event_time: datetime,
    *,
    run_id: Optional[str] = None,
    status: str = "success",
) -> None:
    state = load_state(state_path)
    state.duckdb_loader = DuckDBLoaderState(
        last_loaded_event_time=last_loaded_event_time,
        last_loaded_run_id=run_id,
        status=status,
    )
    save_state(state_path, state)
