from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy.engine import make_url

from src.config import Settings
from src.data_bootstrap import ensure_sample_csvs
from src.db import EngineFactory, set_search_path_safely
from src.seed import load_tables

DBT_DIR = Path(__file__).resolve().parent / "dbt_dashdash"
TARGET_DIR = DBT_DIR / "target"

def _ensure_dbt_dir() -> None:
    if not DBT_DIR.exists():
        raise SystemExit("dbt project directory not found. Expected at dbt_dashdash/.")

def _run_command(args: list[str]) -> None:
    try:
        completed = subprocess.run(
            args,
            cwd=DBT_DIR,
            check=False,
            text=True,
        )
    except FileNotFoundError as exc:  # pragma: no cover - environment dependent
        raise SystemExit("dbt executable not found. Install dbt-core or activate its environment.") from exc
    if completed.returncode != 0:
        raise SystemExit(f"Command {' '.join(args)} failed with exit code {completed.returncode}.")

def _load_results() -> Optional[dict]:
    if not TARGET_DIR.exists():
        return None
    path = TARGET_DIR / "run_results.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None

def _short_name(unique_id: str) -> str:
    parts = unique_id.split(".")
    return parts[-1] if parts else unique_id

def _summarise_results(title: str, results: Optional[dict]) -> None:
    print(f"\n{title}:")
    if not results or "results" not in results:
        print("  • (no results found)")
        return
    for entry in results["results"]:
        name = _short_name(entry.get("unique_id", "unknown"))
        status = entry.get("status", "unknown")
        message = entry.get("message")
        badge = "✅" if status in {"success", "pass"} else "❌"
        detail = f" — {status}"
        if message:
            detail += f" ({message})"
        print(f"  {badge} {name}{detail}")

def _prepare_profiles_dir(explicit_dir: Optional[Path], settings: Settings) -> Path:
    if explicit_dir is not None:
        candidate = explicit_dir / "profiles.yml"
        if candidate.exists():
            return explicit_dir
    url = make_url(settings.database_url)
    backend = url.get_backend_name()
    if backend not in {"postgresql"}:
        raise SystemExit("dbt runner currently supports PostgreSQL targets only.")
    generated_dir = DBT_DIR / "_profiles"
    generated_dir.mkdir(parents=True, exist_ok=True)
    profile_path = generated_dir / "profiles.yml"
    schema = os.getenv("DBT_SCHEMA") or settings.student_netid or "public"
    profile = {
        "dashdash_lab": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": url.host or "localhost",
                    "port": int(url.port or 5432),
                    "user": url.username or "",
                    "password": url.password or "",
                    "dbname": url.database or "postgres",
                    "schema": schema,
                    "threads": int(os.getenv("DBT_THREADS", "4")),
                    "keepalives_idle": 0,
                    "connect_timeout": 10,
                }
            },
        }
    }
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=True), encoding="utf-8")
    return generated_dir

def main() -> None:
    _ensure_dbt_dir()
    settings = Settings.from_env()
    factory = EngineFactory(settings)
    set_search_path_safely(factory)
    ensure_sample_csvs()
    load_tables(factory)
    factory.dispose()

    explicit_dir = os.getenv("DBT_PROFILES_DIR")
    profiles_dir = _prepare_profiles_dir(Path(explicit_dir) if explicit_dir else None, settings)
    target = os.getenv("DBT_TARGET", "dev")
    print("Using dbt project at", DBT_DIR)
    print("Profiles dir:", profiles_dir)
    print("Target:", target)
    if (DBT_DIR / "packages.yml").exists():
        _run_command([
            "dbt",
            "deps",
            "--profiles-dir",
            str(profiles_dir),
        ])
    _run_command([
        "dbt",
        "run",
        "--target",
        target,
        "--profiles-dir",
        str(profiles_dir),
    ])
    _summarise_results("dbt run results", _load_results())
    _run_command([
        "dbt",
        "test",
        "--target",
        target,
        "--profiles-dir",
        str(profiles_dir),
    ])
    _summarise_results("dbt test results", _load_results())

if __name__ == "__main__":
    main()
