# Runner that dispatches to either the plain SQL pipeline or the dbt workflow.
import argparse
import sys

from main_plain_sql import main as run_plain_sql


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="DashDash lab runner")
    parser.add_argument(
        "mode",
        choices=["plain", "dbt"],
        default="plain",
        nargs="?",
        help="Choose 'plain' for the Python+SQL pipeline or 'dbt' to run the dbt project.",
    )
    args = parser.parse_args(argv)

    if args.mode == "plain":
        run_plain_sql()
    else:
        from main_dbt import main as run_dbt

        run_dbt()


if __name__ == "__main__":
    main(sys.argv[1:])
