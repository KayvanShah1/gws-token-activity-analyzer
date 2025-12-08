"""
Soft reset for processed + loader steps.

- Clears processed parquet outputs so they can be regenerated with updated schemas.
- Resets only the processor and DuckDB loader portions of state (keeps fetcher cursors intact).
- Drops the DuckDB loader schema to force downstream models/tables to be rebuilt.
"""

import logging
import shutil
from pathlib import Path

import duckdb
from rich.console import Console

from gws_pipeline.core import settings
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.schemas.state import DuckDBLoaderState, ProcessorState
from gws_pipeline.core.state import load_state, save_state

console = Console()


def _rel(path: Path) -> Path:
    """Return path relative to project base if possible."""
    try:
        return path.relative_to(settings.base_dir)
    except ValueError:
        return path


def _clear_processed_data() -> None:
    processed_dir = settings.processed_data_dir
    if processed_dir.exists():
        shutil.rmtree(processed_dir)
        console.print(f"[green]Cleared processed data:[/green] {_rel(processed_dir)}")
    else:
        console.print(f"[yellow]Skip (missing processed dir):[/yellow] {_rel(processed_dir)}")
    processed_dir.mkdir(parents=True, exist_ok=True)


def _reset_processor_and_loader_state() -> None:
    for app in Application:
        state_path = settings.state_dir / f"{app.value.lower()}.json"
        if not state_path.exists():
            console.print(f"[yellow]Skip state reset (missing file):[/yellow] {_rel(state_path)}")
            continue

        state = load_state(state_path)
        state.processor = ProcessorState()
        state.duckdb_loader = DuckDBLoaderState()
        save_state(state_path, state)
        console.print(f"[green]Reset processor/loader state:[/green] {_rel(state_path)}")


def _drop_loader_schema() -> None:
    schema = settings.DUCKDB_LOADER_SCHEMA
    console.print(f"[cyan]Dropping DuckDB schema '{schema}' (models/tables)...[/cyan]")

    conn = None
    try:
        conn = duckdb.connect(database=settings.duckdb_connection_string)
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        console.print(f"[green]Dropped schema:[/green] {schema}")
    except Exception as exc:
        console.print(f"[red]Failed to drop schema '{schema}':[/red] {exc}")
    finally:
        if conn is not None:
            conn.close()


def soft_reset_processed_and_loader() -> None:
    console.print("[cyan]Soft reset (processed + loader) starting...[/cyan]")
    logging.shutdown()

    _clear_processed_data()
    _reset_processor_and_loader_state()
    _drop_loader_schema()

    console.print("[cyan]Soft reset done.[/cyan]")


if __name__ == "__main__":
    soft_reset_processed_and_loader()
