# 🧠 Project Decision Log & Architecture Reflections

## 📆 1. Overview of Pipeline Design

```text
Google Admin Reports API
        ↓
 Buffered API Fetcher with Retry & Overlap Logic
        ↓
 Partitioned Raw Files (JSONL, per hour)
        ↓
 Beam-based Transformer Pipeline
        ↓
 Partitioned Parquet Files (per day)
        ↓
 Polars-based Ad Hoc Analytics
```

---

## 🎯 2. Data Ingestion Design

### ✅ Chosen Approach

* Authorized Google API session with retry strategy (exponential backoff).
* Use of a **state checkpoint file** to fetch data since the last run.
* Add a **3-minute overlap** to handle delayed/reordered events.
* Buffered writes to hourly-partitioned **JSONL files**.
* Per-run **gzip logs** for debugging/audit.

### 💭 Explored

* Initially stored one file per request → noisy.
* Buffering improved organization and reduced I/O.

### 💡 Key Takeaways

* Overlap + dedup = reliable recovery.
* Per-run logs = forensic aid + resumability.

---

## ⚙️ 3. Data Transformation & Processing

### ✅ Final Tool: **Apache Beam** (DirectRunner)

* Ideal for **parallel processing** and **grouped deduplication**.
* Easier to write **daily-partitioned Parquet**.

### 🧪 Tried Before:

* **Polars / Pandas**:

  * ❌ Inefficient for dedup + grouping large JSONL files.
  * ❌ Not built for partitioned file outputs.
* **Pure Python**:

  * ❌ Memory heavy, non-parallel, not scalable.
* **PySpark**:

  * ❌ Setup overhead, overkill for batch size.

### 💡 Nuance

* Beam slower (\~15 mins), but **scalable + extensible**.
* Polars better for **ad-hoc analysis**.

---

## 📀 4. Data Storage Strategy

* **Raw Events**: `data/raw/YYYY-MM-DD/part_HH.jsonl.gz`
* **Processed**: `data/processed/events_YYYY-MM-DD.parquet`
* **Audit Logs**: `data/per_run/epr_<timestamp>.jsonl.gz`
* **State File**: `state/fetcher_state.json`

### 🧠 Tradeoffs

* Considered DBs (SQLite, TimescaleDB):

  * ❌ Setup burden, not ideal for batch ingest.
* Filesystem layout = optimal for batch + local dev.

---

## 📊 5. Analytics Layer

### ✅ Tool: **Polars**

* Blazing fast.
* Efficient for large Parquet files.
* Easy grouping/aggregation.

### Alternatives

* **DuckDB**:

  * 🟡 Fast, SQL-native, but lacks JSON support.
  * ✅ Could complement Polars in next iteration.

---

## 🛠️ 6. Observability & Logging

* Used `logging` module with `RotatingFileHandler`.
* Rich logs for console readability.
* Tracks file paths, retries, durations, buffer sizes.

---

## 🧪 7. Testing and Safety

* Buffered writes: 5K (hour), 2.5K (per-run).
* Manual inspection confirms no data loss/dup.
* Future enhancement: **schema drift alerts** with Pydantic.

---

## ⚡️ 8. Performance & Compression

* Used `.jsonl.gz`:

  * ✅ Easy to read/stream.
  * ⚠️ `gzip` is slow. Consider `zstd`/`brotli`.
* Parquet: compressed with `snappy`.
* Add `metrics.md` to track throughput/timings.

---

## 💧 9. Beam for GCP Fit

* Beam integrates with **Dataflow** for cloud scaling.
* Excellent for **audit-type pipelines**.
* Chosen for assignment's **GCP-native** focus.

---

## ⚖️ 10. Tools Comparison Snapshot

| Feature             | Pure Python | PySpark    | Polars      | Apache Beam (✅) |
| ------------------- | ----------- | ---------- | ----------- | --------------- |
| Easy to get started | ✅           | ❌          | ✅           | ⚠️ (moderate)   |
| Handles JSONL       | ⚠️          | ✅ (schema) | ⚠️ (nested) | ✅               |
| Dedup + Grouping    | ❌           | ✅          | ❌ (manual)  | ✅               |
| Partitioned Output  | ❌           | ✅          | ⚠️ (manual) | ✅               |

---

## 🔊 11. Edge Case Discussions

* **Backfill Mode**: add CLI override.
* **Replay Mode**: `--replay YYYY-MM-DD [--hour HH]`
* **Tentative State Save**: update state only after successful flush.
* **Overlap Efficiency**: log when duplicates are caught.
* **Ordering Assurance**: detect out-of-order timestamps.

---

## 🔑 12. Security Considerations

* `actor.email` = PII. Hash/anonymize in prod.
* Use **Secret Manager** for credentials.
* Limit IAM roles strictly.

---

## 📊 13. Analysis Extensions

* User activity trends.
* Volume anomaly detection.
* Most frequent `method_name` per user/timeframe.
* Cache summary stats using Polars.

---

## 🚀 14. Future Enhancements

| Feature                      | Reason                                        |
| ---------------------------- | --------------------------------------------- |
| `main.py` orchestrator       | Cleaner CLI or scheduled entrypoint           |
| CI/CD                        | Linting, testing, scheduling (GitHub Actions) |
| Data catalog/schema registry | Schema versioning, discoverability            |
| Partition compaction         | Merge hourly JSONLs into daily Parquet        |
| Metadata manifest            | Store run stats, file counts, status          |
| Real-time alerting           | Hook into Stackdriver / Grafana               |

---

## 🔄 15. Optional Logging Improvements

| Log Addition            | Purpose                        |
| ----------------------- | ------------------------------ |
| Hourly event count      | See hourly density             |
| Retry count per request | Throttle analysis              |
| Overlap dedup count     | Effectiveness of buffer window |
| Partition write metrics | Write rate / size              |
| GZIP write time         | Bottleneck visibility          |

---

## 🏆 16. Final Tips for Interview/Presentation

* Emphasize scale-vs-simplicity choices.
* Highlight tradeoffs + readiness for real production.
* Use CLI demo to walk through fetch → process → analyze.
* Mention Beam → Dataflow portability.
* Share insights on **observability**, **fault recovery**, **schema safety**.

---

## ✅ TL;DR: Top Points to Include

1. Beam chosen for scalability + GCP fit.
2. Buffered fetch with overlap for resiliency.
3. Structured logging + per-run audit files.
4. Parquet for analytics, Polars for fast exploration.
5. Schema validation + potential drift detection.
6. Options to replay/backfill/debug.
7. Thoughtful tradeoffs at every step.
8. Designed for easy migration to production.

