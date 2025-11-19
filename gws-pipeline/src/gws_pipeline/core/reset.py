import shutil
from pathlib import Path

from .config import settings


def remove_dir_content(path: Path):
    if path.exists():
        shutil.rmtree(path)
        print(f"Cleared: {path.relative_to(Path.cwd())}")


def reset_all():
    print("Full reset starting...")
    remove_dir_content(settings.log_dir)
    remove_dir_content(settings.state_dir)
    remove_dir_content(settings.data_dir)
    print("Full reset done.")


if __name__ == "__main__":
    reset_all()
