from __future__ import annotations

from src.config import Settings, mask_url
from src.pipeline import run_pipeline


def choose_settings() -> Settings:
    settings = Settings.from_env()
    try:
        choice = input("Run pipeline against PostgreSQL? [Y/n]: ").strip().lower()
    except EOFError:
        choice = ""
    if choice in {"n", "no"}:
        settings = settings.for_sqlite()
        print("Using SQLite backend at", settings.database_url)
    else:
        print("Using PostgreSQL backend at", mask_url(settings.database_url))
    return settings


def _render_test_results(title: str, results: list[dict]) -> None:
    print(f"\n{title}:")
    if not results:
        print("  • (no tests found)")
        return
    for result in results:
        badge = "✅" if result["failures"] == 0 else ("❌" if result["severity"] == "error" else "🔶")
        detail = f" — {result['failures']} failing rows"
        if result.get("failure_table"):
            detail += f" (stored in {result['failure_table']})"
        print(f"  {badge} {result['name']}{detail}")


def print_pipeline_summary(summary: dict) -> None:
    print("\n=== Pipeline Summary ===")

    print("\nSeeded tables:")
    for table, count in summary.get("row_counts", {}).items():
        print(f"  • {table}: {count}")

    print("\nRow count verification:")
    for table, count in summary.get("verified_counts", {}).items():
        print(f"  • {table}: {count}")

    _render_test_results("Staging tests", summary.get("staging_tests", []))
    _render_test_results("Mart tests", summary.get("mart_tests", []))
    _render_test_results("Custom tests", summary.get("custom_tests", []))

    print("\nExports:")
    exports = summary.get("exports", {})
    if not exports:
        print("  • (no exports generated)")
    else:
        for view, count in exports.items():
            print(f"  • {view}: {count} rows")

    reply = summary.get("stakeholder_reply")
    if reply is not None:
        print(f"\nStakeholder reply: {reply}")

    run_log = summary.get("run_log")
    if run_log is not None:
        print(f"Run log: {run_log}")


def main() -> None:
    settings = choose_settings()
    summary = run_pipeline(settings=settings)
    print_pipeline_summary(summary)


if __name__ == "__main__":
    main()
