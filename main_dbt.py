from __future__ import annotations

import importlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from src.config import Settings
from src.data_bootstrap import ensure_sample_csvs
from src.db import EngineFactory, set_search_path_safely
from src.seed import load_tables

DBT_DIR = Path(__file__).resolve().parent / "dbt_dashdash"
TARGET_DIR = DBT_DIR / "target"

def _ensure_dbt_dir() -> None:
    if not DBT_DIR.exists():
        raise SystemExit("dbt project directory not found. Expected at dbt_dashdash/.")

def _ensure_distutils_available(env: dict[str, str]) -> dict[str, str]:
    if importlib.util.find_spec("distutils") is not None:  # pragma: no cover - depends on runtime
        return env

    shim_root = Path(__file__).resolve().parent / "python_shims"
    shim_path = str(shim_root)
    current = env.get("PYTHONPATH")
    env["PYTHONPATH"] = shim_path if not current else os.pathsep.join([shim_path, current])
    return env


def _run_command(args: list[str], *, allow_failure: bool = False) -> None:
    try:
        env = _ensure_distutils_available(os.environ.copy())
        completed = subprocess.run(
            args,
            cwd=DBT_DIR,
            check=False,
            text=True,
            env=env,
        )
    except FileNotFoundError as exc:  # pragma: no cover - environment dependent
        raise SystemExit("dbt executable not found. Install dbt-core or activate its environment.") from exc
    if completed.returncode != 0:
        if allow_failure:
            print(
                f"Command {' '.join(args)} failed with exit code {completed.returncode}, continuing because allow_failure=True."
            )
            return
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

class ConnectionFailedError(RuntimeError):
    """Raised when a database connection cannot be established."""


def _bootstrap_database(settings: Settings) -> None:
    """Create seed data in the configured database, validating connectivity."""

    factory = EngineFactory(settings)
    try:
        try:
            with factory.connect():
                pass
        except SQLAlchemyError as exc:
            raise ConnectionFailedError("Unable to connect to database") from exc

        set_search_path_safely(factory)
        ensure_sample_csvs()
        load_tables(factory)
    finally:
        factory.dispose()


def _prepare_profiles_dir(explicit_dir: Optional[Path], settings: Settings) -> Path:
    if explicit_dir is not None:
        candidate = explicit_dir / "profiles.yml"
        if candidate.exists():
            return explicit_dir
    url = make_url(settings.database_url)
    backend = url.get_backend_name()
    generated_dir = DBT_DIR / "_profiles"
    generated_dir.mkdir(parents=True, exist_ok=True)
    profile_path = generated_dir / "profiles.yml"
    schema = os.getenv("DBT_SCHEMA") or settings.student_netid or "public"

    if backend == "postgresql":
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
    elif backend == "sqlite":
        sqlite_schema = os.getenv("DBT_SQLITE_SCHEMA") or "main"
        database_path = url.database
        if not database_path:
            raise SystemExit("SQLite database path missing from DATABASE_URL.")
        profile = {
            "dashdash_lab": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "sqlite",
                        "threads": int(os.getenv("DBT_THREADS", "4")),
                        "database": database_path,
                        "schema": sqlite_schema,
                    }
                },
            }
        }
    else:
        raise SystemExit("dbt runner supports PostgreSQL and SQLite targets only.")
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=True), encoding="utf-8")
    return generated_dir

def main() -> None:
    _ensure_dbt_dir()
    settings = Settings.from_env()
    url = make_url(settings.database_url)

    try:
        _bootstrap_database(settings)
    except ConnectionFailedError:
        if url.get_backend_name() == "postgresql":
            print("PostgreSQL connection failed. Falling back to SQLite backend.")
            settings = settings.for_sqlite()
            print("Using SQLite backend at", settings.database_url)
            _bootstrap_database(settings)
        else:
            raise

    explicit_dir = os.getenv("DBT_PROFILES_DIR")
    profiles_dir = _prepare_profiles_dir(Path(explicit_dir) if explicit_dir else None, settings)
    target = os.getenv("DBT_TARGET", "dev")
    print("Using dbt project at", DBT_DIR)
    print("Profiles dir:", profiles_dir)
    print("Target:", target)
    if (DBT_DIR / "packages.yml").exists():
        _run_command(
            [
                "dbt",
                "deps",
                "--profiles-dir",
                str(profiles_dir),
            ],
            allow_failure=True,
        )
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
