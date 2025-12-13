"""
Estimate backward overlap minutes by analyzing per-run snapshots.

- Scans per-app snapshot JSON files under `var/data/runs_snapshots/<app>/`.
- Validates snapshot timing fields with a lightweight Pydantic schema.
- Computes head/tail gaps per app (in minutes), recommends overlap minutes, and reports an aggregate.
"""

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.table import Table

from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.utils import timed_run

logger = get_logger("EstimateOverlap")
console = Console()


# --------------------------------------------------------------------------- #
# Models                                                                      #
# --------------------------------------------------------------------------- #


class SnapshotTiming(BaseModel):
    """Minimal snapshot timing schema."""

    start: datetime
    end: datetime
    earliest_event_time: datetime
    latest_event_time: datetime

    model_config = {"extra": "ignore"}

    def to_utc(self) -> "SnapshotTiming":
        return SnapshotTiming(
            start=_as_utc(self.start),
            end=_as_utc(self.end),
            earliest_event_time=_as_utc(self.earliest_event_time),
            latest_event_time=_as_utc(self.latest_event_time),
        )


class RunStats(BaseModel):
    files_considered: int
    runs_used: int


class GapDistribution(BaseModel):
    # All gap stats are in minutes
    p: float
    min: float
    mean: float
    max: float


class AppStats(BaseModel):
    runs: RunStats
    head_gap_minutes: Optional[GapDistribution] = None
    tail_gap_minutes: Optional[GapDistribution] = None
    earliest_event_time: Optional[datetime] = None
    latest_event_time: Optional[datetime] = None


class AppOverlapResult(BaseModel):
    recommended_backward_overlap_minutes: int
    stats: AppStats
    snapshots_dir: str


class AggregateSummary(BaseModel):
    apps_considered: int
    apps_with_runs: int
    total_files_considered: int
    total_runs_used: int
    # This is max of per-app recommended_backward_overlap_minutes (after safety pad & cap)
    recommended_max_minutes: int
    # Duration between global min earliest_event_time and max latest_event_time, in minutes
    total_duration_minutes: Optional[float] = None


class OverlapConfig(BaseModel):
    percentile: int
    safety_pad_minutes: float
    max_overlap_minutes: Optional[int]
    snapshots_root: str


class EstimateOverlapResponse(BaseModel):
    # Aggregate recommended overlap (same as AggregateSummary.recommended_max_minutes)
    recommended_backward_overlap_minutes: int
    per_app: Dict[str, AppOverlapResult]
    aggregate: AggregateSummary
    config: OverlapConfig


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _percentile(values: list[float], p: float) -> float:
    """Compute the p-th percentile (0-100) with linear interpolation."""
    if not values:
        return 0.0
    v = sorted(values)
    k = (len(v) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return v[int(k)]
    return v[f] + (k - f) * (v[c] - v[f])


def _as_utc(dt: datetime) -> datetime:
    """Normalize any datetime to UTC (naive treated as UTC)."""
    utc = ZoneInfo("UTC")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=utc)
    return dt.astimezone(utc)


def _gap_distribution(values: list[float], percentile: int) -> Optional[GapDistribution]:
    if not values:
        return None
    p_val = _percentile(values, percentile)
    return GapDistribution(
        p=round(p_val, 3),
        min=round(min(values), 3),
        mean=round(sum(values) / len(values), 3),
        max=round(max(values), 3),
    )


def _summarize_app(
    app: Application,
    per_run_dir: Path,
    *,
    percentile: int,
    safety_pad_minutes: float,
    max_overlap_minutes: Optional[int],
) -> AppOverlapResult:
    app_dir = per_run_dir / app.value.lower()
    files = sorted(app_dir.glob("*.json")) if app_dir.exists() else []

    head_gaps: list[float] = []
    tail_gaps: list[float] = []
    runs_used = 0
    app_min_earliest: Optional[datetime] = None
    app_max_latest: Optional[datetime] = None

    for fpath in files:
        try:
            snapshot = json.loads(fpath.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("[%s] Skipping malformed/unreadable %s: %s", app.value, fpath, exc)
            continue

        try:
            snap = SnapshotTiming.model_validate(snapshot).to_utc()
        except ValidationError as exc:
            logger.warning("[%s] Skipping snapshot with invalid schema %s: %s", app.value, fpath.name, exc)
            continue

        head_gap = max(0.0, (snap.earliest_event_time - snap.start).total_seconds() / 60.0)
        tail_gap = max(0.0, (snap.end - snap.latest_event_time).total_seconds() / 60.0)

        head_gaps.append(head_gap)
        tail_gaps.append(tail_gap)
        runs_used += 1

        if app_min_earliest is None or snap.earliest_event_time < app_min_earliest:
            app_min_earliest = snap.earliest_event_time
        if app_max_latest is None or snap.latest_event_time > app_max_latest:
            app_max_latest = snap.latest_event_time

    files_considered = len(files)
    head_stats = _gap_distribution(head_gaps, percentile)
    tail_stats = _gap_distribution(tail_gaps, percentile)

    recommended = 0
    if head_stats is not None:
        # percentile-of-head-gap (in minutes) + safety pad
        recommended = int(math.ceil(head_stats.p + safety_pad_minutes))
        # enforce global cap if provided
        if max_overlap_minutes is not None:
            recommended = min(recommended, max_overlap_minutes)

    app_stats = AppStats(
        runs=RunStats(files_considered=files_considered, runs_used=runs_used),
        head_gap_minutes=head_stats,
        tail_gap_minutes=tail_stats,
        earliest_event_time=app_min_earliest,
        latest_event_time=app_max_latest,
    )

    return AppOverlapResult(
        recommended_backward_overlap_minutes=recommended,
        stats=app_stats,
        snapshots_dir=str(app_dir.relative_to(settings.base_dir)) if app_dir.exists() else str(app_dir),
    )


def _build_table(resp: EstimateOverlapResponse) -> Table:
    # Use custom header rows instead of Rich's default header
    table = Table(title="Backward Overlap Recommendation", show_header=False)

    # Columns: App, Files, Runs, 4x Head, 4x Tail, Recommended
    table.add_column("App", style="cyan", no_wrap=True)
    table.add_column("Files", justify="right")
    table.add_column("Runs", justify="right")

    for _ in range(4):
        table.add_column(justify="right")  # Head group

    for _ in range(4):
        table.add_column(justify="right")  # Tail group

    table.add_column("Recommended (min)", justify="right")

    # First header row: group headers
    table.add_row(
        "App",
        "Files",
        "Runs",
        "[bold]Head (min)[/bold]",
        "",
        "",
        "",
        "[bold]Tail (min)[/bold]",
        "",
        "",
        "",
        "Recommended (min)",
    )

    # Second header row: sub-headers
    table.add_row(
        "",
        "",
        "",
        "p",
        "min",
        "mean",
        "max",
        "p",
        "min",
        "mean",
        "max",
        "",
        style="dim",
    )
    table.add_section()

    # Data rows
    for app in Application:
        app_res = resp.per_app.get(app.value)
        if not app_res:
            table.add_row(app.value, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
            continue

        stats = app_res.stats
        head = stats.head_gap_minutes
        tail = stats.tail_gap_minutes

        table.add_row(
            app.value,
            str(stats.runs.files_considered),
            str(stats.runs.runs_used),
            str(head.p) if head else "-",
            str(head.min) if head else "-",
            str(head.mean) if head else "-",
            str(head.max) if head else "-",
            str(tail.p) if tail else "-",
            str(tail.min) if tail else "-",
            str(tail.mean) if tail else "-",
            str(tail.max) if tail else "-",
            str(app_res.recommended_backward_overlap_minutes),
        )

    # Aggregate row: ALL = max per-app recommended overlap (safe global default)
    agg = resp.aggregate
    table.add_section()
    table.add_row(
        "ALL",
        str(agg.total_files_considered),
        str(agg.total_runs_used),
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        str(agg.recommended_max_minutes),
        style="bold white",
    )

    return table


def _build_summary_table(resp: EstimateOverlapResponse) -> Table:
    agg = resp.aggregate
    cfg = resp.config

    summary = Table(title="Config & Observed Duration", show_header=False)
    summary.add_row("Percentile (head gap)", str(cfg.percentile))
    summary.add_row("Safety pad (min)", f"{cfg.safety_pad_minutes}")
    summary.add_row(
        "Max overlap cap (min)",
        str(cfg.max_overlap_minutes) if cfg.max_overlap_minutes is not None else "None",
    )
    summary.add_row("Snapshots root", cfg.snapshots_root)
    summary.add_row(
        "Apps with runs / considered",
        f"{agg.apps_with_runs} / {agg.apps_considered}",
    )
    if agg.total_duration_minutes is not None:
        days = agg.total_duration_minutes / (60.0 * 24.0)
        summary.add_row(
            "Observed duration (~days)",
            f"{days:.2f}",
        )
    else:
        summary.add_row("Observed duration (~days)", "N/A")

    summary.add_row(
        "Global recommended overlap (min)",
        str(resp.recommended_backward_overlap_minutes),
    )

    return summary


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #


@timed_run
def estimate_backward_overlap(
    run_window_minutes: Optional[int] = None,  # deprecated, kept for compatibility
    percentile: int = 95,
    safety_pad_minutes: float = 1.0,
    max_overlap_minutes: int = 10,
) -> EstimateOverlapResponse:
    """
    Estimate a safe backward overlap (in minutes) for fetch jobs by measuring head/tail gaps per app.

    Returns a typed response with per-app recommendations plus an aggregate (max of per-app).
    """
    if run_window_minutes is not None:
        logger.warning("'run_window_minutes' is deprecated for this script; window times are read from snapshots.")

    per_run_dir = Path(settings.per_run_data_dir)
    if not per_run_dir.exists():
        logger.warning("Snapshots directory missing at %s", per_run_dir)
        empty_per_app: Dict[str, AppOverlapResult] = {}
        aggregate = AggregateSummary(
            apps_considered=len(Application),
            apps_with_runs=0,
            total_files_considered=0,
            total_runs_used=0,
            recommended_max_minutes=0,
            total_duration_minutes=None,
        )
        config = OverlapConfig(
            percentile=percentile,
            safety_pad_minutes=safety_pad_minutes,
            max_overlap_minutes=max_overlap_minutes,
            snapshots_root=str(per_run_dir),
        )
        return EstimateOverlapResponse(
            recommended_backward_overlap_minutes=0,
            per_app=empty_per_app,
            aggregate=aggregate,
            config=config,
        )

    per_app_results: Dict[str, AppOverlapResult] = {}
    recommended_values: list[int] = []
    apps_with_runs = 0
    total_files_considered = 0
    total_runs_used = 0
    global_min_earliest: Optional[datetime] = None
    global_max_latest: Optional[datetime] = None

    for app in Application:
        result = _summarize_app(
            app,
            per_run_dir,
            percentile=percentile,
            safety_pad_minutes=safety_pad_minutes,
            max_overlap_minutes=max_overlap_minutes,
        )
        per_app_results[app.value] = result

        stats = result.stats
        total_files_considered += stats.runs.files_considered
        total_runs_used += stats.runs.runs_used

        if stats.runs.runs_used > 0:
            apps_with_runs += 1
            recommended_values.append(result.recommended_backward_overlap_minutes)

            if stats.earliest_event_time is not None:
                if global_min_earliest is None or stats.earliest_event_time < global_min_earliest:
                    global_min_earliest = stats.earliest_event_time
            if stats.latest_event_time is not None:
                if global_max_latest is None or stats.latest_event_time > global_max_latest:
                    global_max_latest = stats.latest_event_time

    aggregate_recommended = max(recommended_values) if recommended_values else 0

    total_duration_minutes: Optional[float] = None
    if global_min_earliest is not None and global_max_latest is not None:
        total_duration_minutes = (global_max_latest - global_min_earliest).total_seconds() / 60.0

    aggregate = AggregateSummary(
        apps_considered=len(Application),
        apps_with_runs=apps_with_runs,
        total_files_considered=total_files_considered,
        total_runs_used=total_runs_used,
        recommended_max_minutes=aggregate_recommended,
        total_duration_minutes=total_duration_minutes,
    )
    config = OverlapConfig(
        percentile=percentile,
        safety_pad_minutes=safety_pad_minutes,
        max_overlap_minutes=max_overlap_minutes,
        snapshots_root=str(per_run_dir.relative_to(settings.base_dir)),
    )

    return EstimateOverlapResponse(
        recommended_backward_overlap_minutes=aggregate_recommended,
        per_app=per_app_results,
        aggregate=aggregate,
        config=config,
    )


if __name__ == "__main__":
    response = estimate_backward_overlap()
    console.print(_build_table(response))
    console.print()
    console.print(_build_summary_table(response))
