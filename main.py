import argparse
from src.report import run_all


def parse_args():
    parser = argparse.ArgumentParser(description="Run UTM Attribution Funnel pipeline")
    parser.add_argument(
        "--data",
        default="data/DataTask.csv",
        help="Path to the input CSV dataset",
    )
    parser.add_argument(
        "--out",
        default="outputs/",
        help="Directory to save outputs (CSVs, charts, and DuckDB file)",
    )
    parser.add_argument(
        "--no-duckdb",
        action="store_true",
        help="Disable saving data model to DuckDB",
    )
    parser.add_argument(
        "--duckdb-path",
        default=None,
        help="Optional custom path for the DuckDB file (default: outputs/duckdb/attribution.duckdb)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    artifacts = run_all(
        dataset_path=args.data,
        outputs_dir=args.out,
        save_duckdb=not args.no_duckdb,
        duckdb_path=args.duckdb_path,
    )

    print("Pipeline completed successfully!")
    print("Generated artifacts:")
    for key, value in artifacts.items():
        print(f" - {key}: {value}")


if __name__ == "__main__":
    main()