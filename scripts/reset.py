import logging
import shutil
import sys
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gws-pipeline" / "src"))
from gws_pipeline.core.config import settings

console = Console()


def remove_dir_content(path: Path):
    if path.exists():
        shutil.rmtree(path)
        console.print(f"[green]Cleared:[/green] {path.relative_to(Path.cwd())}")
    else:
        console.print(f"[yellow]Skip (missing):[/yellow] {path.relative_to(Path.cwd())}")


def reset_all():
    console.print("[cyan]Full reset starting...[/cyan]")
    logging.shutdown()  # close handlers before deleting logs
    remove_dir_content(settings.log_dir)
    remove_dir_content(settings.state_dir)
    remove_dir_content(settings.data_dir)
    console.print("[cyan]Full reset done.[/cyan]")


if __name__ == "__main__":
    reset_all()
