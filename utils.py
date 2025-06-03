import json
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

from config import get_logger, settings

logger = get_logger("TimedRun")


def get_relative_path(file_path: str, base: str = Path.cwd()) -> str:
    try:
        return str(Path(file_path).relative_to(base))
    except ValueError:
        return str(Path(file_path))


def format_duration(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes:02}m {seconds:02}s"


def fetch_last_run_timestamp(path=settings.state_file_fetcher) -> datetime:
    with open(path, "r") as f:
        ts = datetime.fromisoformat(json.load(f)["last_run"])
    return ts


def timed_run(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now(timezone.utc)
        logger.info(f"Started: {func.__name__}")
        result = func(*args, **kwargs)
        duration = datetime.now(timezone.utc) - start_time
        logger.info(f"Completed: {func.__name__} in {format_duration(duration)}.")
        return result

    return wrapper
