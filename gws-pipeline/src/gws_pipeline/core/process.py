import glob
import gzip
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.options.pipeline_options import PipelineOptions

from .config import get_logger, settings
from .models import RawTokenActivity
from .utils import fetch_last_run_timestamp, format_duration

logger = get_logger("TokenActivityProcessor")

PARQUET_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us")),
        ("unique_id", pa.string()),
        ("user", pa.string()),
        ("profile_id", pa.string()),
        ("ip", pa.string()),
        ("event_type", pa.string()),
        ("event_name", pa.string()),
        ("method_name", pa.string()),
        ("num_bytes", pa.int64()),
        ("api_name", pa.string()),
        ("client_id", pa.string()),
        ("app_name", pa.string()),
        # ("client_type", pa.string()),
        # ("product_bucket", pa.string()),
    ]
)


class ReadCompressedJSONL(beam.DoFn):
    def process(self, file_path: str):
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                yield line


class ParseAndFlatten(beam.DoFn):
    def process(self, line):
        try:
            raw = json.loads(line)
            activity = RawTokenActivity(**raw)
            yield activity.to_flat_dict()
        except Exception as e:
            yield beam.pvalue.TaggedOutput("errors", {"error": str(e), "raw": line})


class WritePartitionedParquet(beam.DoFn):
    def __init__(self, output_dir: str, schema: pa.Schema):
        self.output_dir = output_dir
        self.schema = schema

    def process(self, element):
        date_key, records = element
        if not records:
            return

        output_path = Path(self.output_dir) / f"{date_key}.parquet"
        os.makedirs(output_path.parent, exist_ok=True)

        table = pa.Table.from_pylist(records, schema=self.schema)
        pq.write_table(table, output_path, compression="snappy")

        yield f"Wrote {len(records)} rows to {output_path.relative_to(settings.base_dir)}"


def extract_partition_key(row):
    dt = row["timestamp"].strftime("%Y-%m-%d")
    return f"events_{dt}", row


def get_recent_files(base_dir: Path, hours: int = 48):
    last_run = fetch_last_run_timestamp()
    start_time = last_run - timedelta(hours=hours)

    matching_files = []

    for i in range((last_run - start_time).days + 1):
        day = (start_time + timedelta(days=i)).strftime("%Y-%m-%d")
        day_dir = base_dir / day
        pattern = str(day_dir / "*.jsonl.gz")
        matching_files.extend(glob.glob(pattern))

    return matching_files


def default_parallelism():
    cpus = os.cpu_count() or 2
    return max(2, int(cpus * 0.50))


def run():
    logger.info("Processing Pipeline started")
    run_start_time = datetime.now(timezone.utc)

    recent_files = get_recent_files(settings.raw_data_dir, hours=settings.DEFAULT_DELTA_HRS)
    if not recent_files:
        logger.warning("No files found for the last 48 hours.")
        return
    output_base = settings.processed_data_dir

    options = PipelineOptions([], save_main_session=False)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Create file paths" >> beam.Create(recent_files)
            | "Read GZipped JSONL" >> beam.ParDo(ReadCompressedJSONL())
            | "Parse & Flatten" >> beam.ParDo(ParseAndFlatten())
            | "Dedup by unique_id" >> beam.Map(lambda x: (x["unique_id"], x))
            | "Get unique" >> beam.CombinePerKey(lambda vals: list(vals)[0])
            | "Extract" >> beam.Values()
            | "Key by partition" >> beam.Map(extract_partition_key)
            | "Group by partition" >> beam.GroupByKey()
            | "Write partitioned Parquet"
            >> beam.ParDo(WritePartitionedParquet(output_dir=output_base, schema=PARQUET_SCHEMA))
            | "Log output" >> beam.Map(lambda msg: logger.info(msg))
        )

    time_elapsed = format_duration(datetime.now(timezone.utc) - run_start_time)
    logger.info(f"Processing Pipeline completed in {time_elapsed}.")


if __name__ == "__main__":
    run()
