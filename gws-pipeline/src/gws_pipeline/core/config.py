from datetime import timezone
import logging
import logging.handlers
import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from rich.logging import RichHandler


class AppSettings(BaseSettings):
    # Base directories
    base_dir: Path = Path(__file__).resolve().parents[4]

    # File paths for data storage
    data_dir: Path = Path.joinpath(base_dir, "data")
    state_dir: Path = Path.joinpath(base_dir, "state")
    raw_data_dir: Path = Path.joinpath(data_dir, "raw")
    per_run_data_dir: Path = Path.joinpath(data_dir, "runs")
    processed_data_dir: Path = Path.joinpath(data_dir, "processed")
    log_dir: Path = Path.joinpath(base_dir, "logs")

    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)

    # State files for tracking last run timestamps
    state_file_fetcher: Path = Path.joinpath(state_dir, "last_run_fetcher.json")

    # Environment-specific settings
    BUFFER_SIZE: int = Field(5000, description="Buffer size for batch partition flush")
    PER_RUN_BUFFER_SIZE: int = Field(2500, description="Buffer size for per-run log file")
    DEFAULT_TIMEZONE: timezone = Field(timezone.utc, description="Timezone for datetime handling")
    DEFAULT_DATE_FORMAT: str = Field("%Y-%m-%dT%H:%M:%S%z", description="Format for datetime parsing")
    DEFAULT_DELTA_HRS: int = Field(48, description="Default hours back to fetch on first run")
    OVERLAP_MINUTES: int = Field(3, description="Overlap buffer for safe backfill")
    USE_GZIP: bool = Field(False, description="Whether to gzip output files")
    GZIP_COMPRESSION_LVL: int = Field(5, description="Compression level for gzip files")

    # Google Workspace API settings
    base_url: str = Field("https://www.googleapis.com", description="Base URL for Google Workspace API")
    subject: str = Field(..., description="Subject email for impersonation in API requests")
    creds_file: Path = Field(
        default=Path.joinpath(base_dir, "creds.json"), description="Path to Google Workspace credentials file"
    )

    # Configuration for Pydantic settings
    model_config = SettingsConfigDict(
        env_prefix="GWS_", env_file=f"{base_dir}/.env", env_file_encoding="utf-8", extra="ignore"
    )

    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        for k, v in dump.items():
            if isinstance(v, Path) and v.is_absolute():
                try:
                    dump[k] = v.relative_to(self.base_dir)
                except ValueError:
                    pass
        return dump


settings = AppSettings()


def get_logger(name):
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Console handler (Rich)
        ch = RichHandler(rich_tracebacks=True)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("'%(name)s' - %(message)s"))
        logger.addHandler(ch)

        # File handler (Rotating)
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(settings.log_dir, "gws-activity-analyzer.log"),
            maxBytes=50 * 1024,  # 20KB log file max
            backupCount=3,
            delay=True,
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    logger = get_logger("GWSSettings")
    logger.info(f"Settings loaded: \n{settings.model_dump()}")  # Debugging line to check settings
