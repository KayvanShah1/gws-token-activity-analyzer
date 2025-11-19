from datetime import datetime
from dagster import TimeWindowPartitionsDefinition


def get_partitions(start_date: datetime) -> TimeWindowPartitionsDefinition:
    # Each partition is a 4-hour window
    fmt = "%Y-%m-%dT%H:%M"
    partitions = TimeWindowPartitionsDefinition(
        start=start_date.strftime(fmt),  # or earlier if you want full backfill
        cron_schedule="0 */8 * * *",  # every 4 hours
        fmt=fmt,  # partition key format (customizable)
    )
    return partitions
