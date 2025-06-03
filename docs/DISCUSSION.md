# 🧠 Project Decision Log & Architecture Reflections

This document captures the thought process, alternatives considered, and reasons for choosing specific tools and architecture for the gws-token-activity-analyzer project. It's not just a README—it's a **tech journal** of how we designed a production-leaning pipeline with real-world constraints in mind. ✨

## 🛠️ Overview of Pipeline Design

text
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


## 1. 🎯 Data Ingestion Design

### ✅ Chosen Approach:

* Use **Authorized Google API session** with retry strategy (exponential backoff).
* Fetch activity logs since last run using a **state checkpoint file**.
* Add **3-minute overlap** to prevent event loss.
* Store raw events as **JSONL per hour**, using buffered writes.

### 💭 Ideas Explored:

* Saving one file per request was too noisy — switched to partitioned writes with a buffer.
* Also created **per-run JSONL logs** (gzip) for auditing/debugging.

### 💡 Takeaways:

* Adding a time overlap + deduplication downstream = safer against late or re-ordered events.
* Per-run logs can serve as a "recovery checkpoint" or forensic trail if anything breaks.

## 2. ⚙️ Data Transformation & Processing

### ✅ Chosen Tool: **Apache Beam** (with DirectRunner)

### Why Beam?

* **Scalable, parallel processing** with easy deduplication via CombinePerKey.
* Cleaner for grouped outputs (e.g., daily Parquet).
* Each run processes 5–6 files with 5k–20k records each, totaling \~500K records.

### 🧪 Ideas Tried:

* Considered **Polars** or **Pandas** for transformation.

  * ❌ Inefficient for transforming millions of rows with dedup + grouping logic.
  * ❌ Not ideal for parallel writes to partitioned Parquet.
* Beam was **slower** (\~15 mins) than Polars, but gave us better structure and extensibility.

### 💡 Nuance:

* For large-scale daily processing with flexible partitioning: Beam > Polars.
* For ad-hoc slicing/dicing: Polars wins.

## 3. 💾 Data Storage Strategy

* Raw Events: JSONL per hour (data/raw/YYYY-MM-DD/part_HH.jsonl.gz)
* Transformed Events: Parquet per day (data/processed/events_YYYY-MM-DD.parquet)
* Per-run Audit: Gzipped JSONL (data/per_run/epr_<timestamp>.jsonl.gz)
* State File: JSON file storing last-run timestamp (state/fetcher_state.json)

### 🧠 Tradeoffs Considered:

* Could have used a DB (e.g., SQLite, TimescaleDB):

  * ❌ Added infra, queries, schema constraints.
  * ❌ Not ideal for large-scale I/O-based transformation.
* Decided on **filesystem-based datalake** layout, good enough for 2-dayly batch pipeline.

## 4. 📊 Analytics Layer

### ✅ Tool: **Polars**

* Ultra-fast DataFrame queries
* Easily filters & groups large Parquet datasets
* Ideal for batch analytics like:

  * Top users
  * API method with highest bytes
  * Hourly/daily event trends

### Explored Alternatives:

* **DuckDB**:

  * 🟡 Faster for SQL-style queries & joins
  * ❌ No native JSON support (yet), so less useful at ingest stage
  * ✅ Excellent complement to Polars for exploratory or dashboard-style querying

## 5. 🧱 Observability & Logging

* Used **Python’s logging module** with RotatingFileHandler.
* Integrated **Rich** for local console logging with timestamps.
* Logging includes:

  * File writes
  * Run duration
  * Per-run file locations
  * Retry/overlap status

## 6. 🧪 Testing and Safety Measures

* Added buffer flushing with sizes (e.g., 5K per partition, 2.5K per-run).
* Manual testing showed no data loss or duplicates after transformation.
* Logs rotate and persist in /logs.

## 7. 🚀 Future Considerations

| Feature                | Why Consider It?                       |
| ---------------------- | -------------------------------------- |
| main.py orchestrator | Wrap fetch + process + analyze cleanly |
| DuckDB for analytics | Interactive SQL-like analysis layer    |
| API pagination counter | Estimate future load from page tokens  |
| DB support (opt-in)    | For continuous/live streaming systems  |
| Data schema registry   | Track versioning of event formats      |

## 🤹 Summary of Tools

| Task             | Tool Used                 | Reason                            |
| ---------------- | ------------------------- | --------------------------------- |
| Fetch Events     | requests + retry + auth | Resilient API access              |
| Transform/Dedup  | Apache Beam             | Parallel, grouped transformations |
| Save as Parquet  | PyArrow                 | Fast columnar storage             |
| Ad-hoc Analytics | Polars                  | Blazing-fast queries              |
| Buffering        | Lists + flush           | Avoids disk I/O flood             |
| Logging & State  | logging + JSON          | Traceability + Resumability       |

Absolutely, let’s enrich the summary with those key offshoot discussions and design explorations you made while evaluating different approaches.

---

## 🔄 Data Processing Approaches Explored

We explored **multiple data processing frameworks** before settling on Apache Beam. Here’s a breakdown of what was considered, what worked, and what didn’t — with the rationale behind choosing Beam for the core transformation job.

### 🐍 Pure Python Approach (Baseline)

* **What we tried**: Looping over GZipped JSONL files, parsing, deduplicating, and writing to Parquet manually using PyArrow.
* **Pros**:

  * Lightweight and easy to debug.
  * Direct control over logic and edge cases.
* **Cons**:

  * 🐢 **Slow** for large volumes (\~500K+ records).
  * ❌ Memory bottlenecks due to large all_events list.
  * ❌ No native parallelism or partitioned processing.
* **Conclusion**: Great for prototyping, but not scalable.

---

### 🔥 PySpark (RDD/DataFrame API)

* **What we tried**: Using Spark to load raw JSONL files and transform data using RDD and DataFrame operations.
* **Pros**:

  * 🔁 Built-in parallelism and powerful for **huge** datasets.
  * 🔍 Good for structured, tabular data.
* **Cons**:

  * 🧱 Heavy setup: required Java, Spark binaries, or PySpark setup.
  * 😵 Overkill for our batch size (\~0.5–1M records).
  * 🚫 Complex I/O when dealing with JSONL + nested fields.
* **Conclusion**: Good for production-scale big data; **too heavy** for our assignment and local run needs.

---

### 🧩 Apache Beam (Final Choice)

* **What worked**:

  * Out-of-the-box parallelism, batching, and grouping.
  * Easy deduplication via CombinePerKey.
  * Controlled partitioned writes per day using Beam transforms.
* **Tradeoffs**:

  * Slightly more setup than pure Python.
  * Slower than Polars in wall-time (\~15 mins for all steps).
  * Still felt “structured” without being too bloated.

> 💡 **Why Beam?**
> It struck the right balance between:

* **Scalability** for future growth.
* **Parallel-friendly** I/O and grouping logic.
* **Ease of local development** without needing a cluster.

---

## 🆚 Why Not Use Spark or Polars for Processing?

| Feature                            | Pure Python | PySpark       | Polars                     | Apache Beam (✅)    |
| ---------------------------------- | ----------- | ------------- | -------------------------- | ------------------ |
| Easy to get started                | ✅           | ❌             | ✅                          | ⚠️ (moderate)      |
| Handles JSONL                      | ⚠️          | ✅ (w/ schema) | ⚠️ (nested is tricky)      | ✅                  |
| Handles dedup + grouping           | ❌           | ✅             | ❌ (manual logic)           | ✅                  |
| Fast for <1M rows                  | ✅           | ❌ (setup tax) | ✅                          | ⚠️ (slightly slow) |
| Works well with partitioned output | ❌           | ✅             | ⚠️ (needs manual batching) | ✅                  |

---

## 🗣️ Side Conversations That Influenced Design

* **Should we use a database?**
  Decided against it due to setup complexity and no real need for real-time queries. Filesystem-based pipeline was leaner.

* **Would DuckDB be better for analysis?**
  Possibly! Especially for SQL-style slicing, but Polars was already performant and fit better with our dev flow.

* **Do we need orchestration?**
  Not for the assignment. But in production, wrapping everything in main.py or using tools like **Airflow**/**Dagster**/**Argo** would help.

Great point — let’s wrap that into the summary to show your architectural foresight. Here's the final updated section for your README-style project notes:

---

## ☁️ Why Apache Beam? Especially for GCP Monitoring?

In the context of a **Google Workspace audit activity monitoring pipeline**, Apache Beam was the most **natural and strategic fit**.

### 🔧 Beam’s Fit for GCP:

* ✅ **Native integration with GCP services**: Beam pipelines can be executed on **Dataflow**, Google’s fully managed distributed processing service.
* ✅ **Scalable for future real-time pipelines**: Although we ran it locally, this pipeline could easily scale to handle millions of rows and stream processing using the same Beam code.
* ✅ **Designed for audit-style workloads**: The Beam model of PCollections and transforms maps very naturally to activity logs, deduplication, grouping, partitioned writes.

### 🧠 Final Thought:

Choosing Beam aligned with the GCP-based nature of the assignment and would allow seamless migration to production in a GCP environment if needed.

---

## ⚖️ Nuanced Trade-offs and Considerations

### 🧩 1. **Apache Beam vs. Other ETL Frameworks**

| Tool            | Pros                                                                 | Cons                                                         | Verdict                                               |
| --------------- | -------------------------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------- |
| **Apache Beam** | Portable, scalable, GCP-friendly, supports batch + stream processing | Slightly steeper learning curve, heavier setup for local dev | ✅ Chosen: Scales to Dataflow, fits GCP audit use case |
| **Pandas**      | Fast prototyping, rich ecosystem                                     | Not memory-efficient for large-scale JSONL; single-machine   | ❌ Not suitable for heavy or long-term workloads       |
| **Polars**      | Fast, efficient, great for columnar/parquet; simple API              | Limited support for deeply nested JSON structures            | ✅ Used for analytics; ❌ Not for raw parsing           |
| **PySpark**     | Scales massively, supports JSON well                                 | Heavyweight setup, unnecessary for current scale             | ❌ Overkill for this project                           |

---

### 🗂️ 2. **Partitioning Strategy**

* **Time-based partitioning by day and hour**
  ✅ Helps incremental loading and isolates corrupted data.
  ✅ Aligns with common analytics patterns (e.g., hourly dashboards).
  ⚠️ Could lead to small files, which are inefficient for certain engines.

* **Per-run JSONL logs**
  ✅ Adds traceability for auditing and debugging.
  ✅ Great for rerunning specific batches or comparing pipelines.
  ⚠️ May duplicate data already in partitioned files if not filtered.

---

### 📄 3. **Data Storage Format**

| Format      | Reason Used                  | Trade-offs                                             |
| ----------- | ---------------------------- | ------------------------------------------------------ |
| .jsonl.gz | API returns JSON; streamable | ✅ Append-friendly, readable                            |
| .parquet  | For processed analytics      | ✅ Columnar, compact, fast for filters                  |
| ❌ SQL DB    | Not used                     | ⚠️ Setup overhead, unnecessary for read-most workloads |

---

### 🧮 4. **Analytics Layer: Polars vs DuckDB**

* **Polars**: Chosen for performance and simplicity in batch-oriented aggregation.

  * ✅ Efficient in-memory operations
  * ✅ Familiar API for pandas users
  * ⚠️ Lacks seamless nested JSON handling

* **DuckDB**: Discussed as an alternative for ad-hoc SQL-based analysis.

  * ✅ Super fast for Parquet + SQL workflows
  * ✅ Would reduce memory footprint for large joins
  * ❌ Not used this time, but a great pick for next version

---

### 🧠 5. **Buffering Writes**

* **Buffered event writing (5K, 2.5K buffers)**:

  * ✅ Improved write performance
  * ✅ Prevented excessive I/O for every event
  * ✅ Ensured per-run file didn't bloat unmanageably
  * ⚠️ Requires careful flush logic to avoid data loss at shutdown

---

### 🧪 6. **Testing vs Observability**

* **No orchestrator yet** but:

  * ✅ Timestamps persisted in state.json ensure fault-tolerance
  * ✅ Logging shows progress and retry status
  * ⚠️ No real-time metrics unless integrated with tools like Stackdriver, Prometheus

---

### 🔌 7. **Future Considerations**

| Enhancement                            | Reason to Consider                       |
| -------------------------------------- | ---------------------------------------- |
| Use of **DuckDB**                      | More flexible SQL-based exploration      |
| Add **streaming support**              | Realtime or near-realtime alerting       |
| Add **orchestration** (e.g., Airflow)  | For running fetch-process-analyze as DAG |
| Build **data catalog/schema registry** | Improve discoverability and validation   |

## 🔍 Additional Details & Refinements to Include

### 🧱 **File Management / IO Optimizations**

* **Compression trade-offs**:

  * Used .jsonl.gz with gzip for raw logs.

    * ✅ Keeps disk usage small.
    * ⚠️ gzip is slow for compression/decompression compared to zstd or snappy.
    * ❗snappy preferred for Parquet (already used) — might note *why* you kept gzip for raw.
* **Small file problem**:

  * If you reprocess or re-fetch hourly, many small files can degrade performance downstream (e.g., S3 + Spark).
  * 📌 *Future*: Consider compacting hourly JSONL into daily Parquet before long-term retention or analytics.

---

### 🔁 **Retry & Fault Tolerance Enhancements**

* **Resumability edge case**:

  * If the job fails mid-run (e.g., after partial fetch + before flush), the state file may falsely advance. You could:

    * Use **"tentative timestamp advancement"** — save state only *after* successful flush.
    * Consider adding a “checkpointed flush” per buffer.
* **Overlapping logic**:

  * 3-minute overlap is a great default, but:

    * 📌 Consider making it **dynamically adjustable** based on observed skew in event arrival times (e.g., percentile delay).

---

### 🧠 **Model Validation & Schema Drift**

* **Pydantic for schema evolution**:

  * ✅ Already used to validate raw events.
  * You might log or capture fields *not* in the current model (i.e., unexpected keys in events).

    * This helps you detect schema drift silently.
    * Add optional "extra_fields" field to hold unmapped keys or log unknown keys in a side channel.
* **Schema registry (future)**:

  * Versioned Pydantic models with @version tags — this can help compare model changes over time for future-proofing.

---

### 🔧 **Beam Pipeline Enhancements**

* **WritePath customization**:

  * You write daily Parquet files. If you switch to per-hour partitioning (YYYY-MM-DD-HH.parquet) it gives better slicing granularity.
* **Sorting by timestamp before Parquet write**:

  * Not strictly required, but improves query performance on timestamp-based filters.
  * You did add this — great! Just mention it **explicitly** under transformation performance.

---

### 📊 **Analytics Ideas (Polars Layer)**

* Consider adding:

  * **Per-user activity patterns** over time (e.g., login bursts).
  * **Volume anomaly detection** (bytes sent, requests/hour).
  * **Most frequent method_name per hour/day/user**.
* 📈 Could pre-define and cache these as **Polars scripts** or Parquet summary files for dashboarding.

---

### ⚠️ **Duplication Risk Handling**

* **Unique ID + Overlap logic**:

  * You dedup using unique_id — which is perfect.
  * Consider asserting **strict ordering** (or logging violations) during transformation — if timestamps are ever back in time.

---

### 🔐 **Security / Sensitivity Considerations**

* **PII/Email handling**:

  * actor.email is personal info — if storing logs long-term, consider hashing, obfuscating, or protecting with IAM rules.
* ✅ You kept logs local — but good to mention this is **non-production** assumption.

---

### 🚦 **Benchmarking & Performance Metrics**

* If you're performance-conscious, consider:

  * **Fetch throughput (events/sec)**.
  * **Processing rate in Beam (rows/sec)**.
  * **Time to Parquet write (time/file or MB/s)**.
* Even basic timing logs (you have these!) + a metrics.md log can serve as a future benchmark baseline.

---

### 🌐 **Cross-cutting Ideas**

| Area                      | Suggestion                                                                           |
| ------------------------- | ------------------------------------------------------------------------------------ |
| **Partition overwrite**   | Consider writing to temp location → move to final → prevents corruption              |
| **Catalog integration**   | Write .schema.json alongside Parquet → self-describing datasets                    |
| **Command orchestration** | Wrap fetch + transform + analyze into a main.py CLI via typer or fire          |
| **Async fetch**           | Not urgent, but could parallelize pages if response token can be split               |
| **Output manifest**       | Store a manifest.json for each run: what files were written, when, how many events |

---

## 🧾 Optional Log Entries to Consider Adding

You already have great logging. Here are minor refinements:

| Log Additions                         | Purpose                                        |
| ------------------------------------- | ---------------------------------------------- |
| Event counts per hour (before write)  | Visibility into hourly density                 |
| Retry count per request (if retried)  | Helps in diagnosing throttling/API issues      |
| Overlap effectiveness check           | Log if any overlapping event was a duplicate   |
| Partition size stats (before Parquet) | Log how many events written per day file       |
| Gzip write time                       | Useful since this was a performance bottleneck |

---

## 🚀 Future-Proofing for Production

* **Move to Dataflow**:

  * You’re Beam-ready → just change the runner and push the code.
* **Auto-scaling and streaming**:

  * Pipeline logic supports streaming (if API + Source allow).
* **CI/CD**:

  * Add basic GitHub Actions for pytest, lint, and scheduled runs.
* **Metadata manifest**:

  * Store last run, number of events fetched, time taken, output path etc., in a structured metadata file (run_<ts>.json).

---

## ✅ TL;DR: Top Additions Worth Including

1. ✅ Compression tradeoff (gzip vs snappy/zstd).
2. ✅ Future: schema evolution, unknown field handling.
3. ✅ Sorting before Parquet write — log explicitly.
4. ✅ Partition-level metrics (rows, duplicates, write time).
5. ✅ Logs for retries, dedup counts, overlap efficiency.
6. ✅ Manifest or metadata registry per run.
7. ✅ Polars analysis ideas: anomaly detection, aggregation.
8. ✅ Logging violations in ordering if ever happens.
9. ✅ Optionally: metrics.md or benchmark log for future speedups.
10. ✅ Add main.py CLI orchestration entrypoint.

---

## 🧩 Additional Enhancements You Could Mention

### 1. 🔄 **Data Lineage / Provenance**

* **Why?** For audit-focused systems (like token activity), traceability is key.
* **Suggestion:** Add metadata to each Parquet file with:

  * Source file(s)
  * Record count
  * Timestamps (min, max)
  * Processing timestamp
* Could be stored as .meta.json alongside each .parquet.

---

### 2. 🛂 **Permissioning + Key Management**

* You already use a service account, which is great.
* Mention that in production:

  * Credentials should be rotated and stored via **secret managers** (e.g., GCP Secret Manager).
  * **IAM roles** should restrict the scope to only needed audit APIs.

---

### 3. 🧪 **Validation / Backfill Strategy**

* What if the pipeline goes down or state is corrupted?
* Consider adding:

  * **Backfill mode** with a start/end timestamp override.
  * **Sanity checks** like:

    * Event time not in future
    * Partition path matches event time
    * No duplicated event IDs in partition

---

### 4. 🛠️ **Schema Evolution Awareness**

* You already use Pydantic for validation — excellent.
* To go further:

  * Store a **model version** in each record.
  * Maintain versioned schemas (e.g., schema_v1.json, v2/RawTokenActivity) to detect drift.
  * Add an **alert if new fields appear** or expected fields go missing.

---

### 5. 📉 **Compression + Storage Strategy**

* GZIP is okay, but:

  * Consider comparing **zstandard (zstd)** or **brotli** for better compression speed & ratio.
  * You can document **benchmark results** in compression_eval.md.

---

### 6. 🚦 **Fail-safe Execution**

* Currently fetcher.py and processor.py assume everything will complete.
* Add resilience by:

  * Logging **partial failure**
  * Using a **run manifest** (status: success/failed, error messages)
  * **Not updating state** unless all steps succeed

---

### 7. 📊 **Monitoring Dashboard (Future)**

If this were a prod system:

* You could pipe log events (counts, durations, retries) to **Prometheus / Grafana** or **Stackdriver**.
* Helps understand:

  * Is the API getting slower?
  * Are more events fetched per run?
  * Is processing lag increasing?

---

### 8. 🔄 **Replay Mode**

* In a prod system:

  * You may want to **replay a single hour or day** (e.g., if corrupt or missing).
* Add CLI arg to process a specific partition:
  python main.py --replay 2024-05-26 --hour 13

---

## 📌 Bonus: Interview/Presentation Tips

If you're asked about your design in an interview:

* ✅ Emphasize *"decisions were made based on current scale and assignment scope."*
* ✅ Clarify that you’ve thought about **observability, future scaling, and fault tolerance**.
* ✅ Show that the pipeline is **modular** and **ready to grow** into a real-world deployment.