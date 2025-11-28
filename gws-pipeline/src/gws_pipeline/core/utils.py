import json
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

from gws_pipeline.core import get_logger, settings

logger = get_logger("TimedRun")


def get_relative_path(file_path: str, base: str = Path.cwd()) -> str:
    """Get the relative path of file_path with respect to base.
    If file_path is not under base, return the absolute path.
    Args:
        file_path (str): The file path to convert
        base (str): The base path to relativize against
    Returns:
        str: Relative path if possible, else absolute path
    """
    try:
        return str(Path(file_path).relative_to(base))
    except ValueError:
        return str(Path(file_path))


def format_duration(td: timedelta) -> str:
    """Format a timedelta into a human-readable string.
    Args:
        td (timedelta): The duration to format.
    Returns:
        str: Formatted duration string.
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} h {minutes:02} m {seconds:02} s"


def fetch_last_run_timestamp(path=settings.state_file_fetcher) -> datetime:
    """Fetch the last run timestamp from the state file."""
    with open(path, "r") as f:
        ts = datetime.fromisoformat(json.load(f)["last_run"])
    return ts


def timed_run(func):
    """Decorator to time the execution of a function and log its duration.
    Args:
        func (Callable): The function to be decorated.
    Returns:
        Callable: The wrapped function with timing.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now(timezone.utc)
        logger.info(f"Started: {func.__name__}")
        result = func(*args, **kwargs)
        duration = datetime.now(timezone.utc) - start_time
        logger.info(f"Completed: {func.__name__} in {format_duration(duration)}.")
        return result

    return wrapper
