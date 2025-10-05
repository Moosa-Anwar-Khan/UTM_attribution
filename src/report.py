from pathlib import Path
import pandas as pd

from .preprocess import clean_dataframe, parse_datetimes, drop_impossible_dates
from .modeling import build_contacts, extract_utm, build_events, rollup_users
from .metrics import metrics_per_utm, event_category_mix
from .visuals import chart_bar
from .duckdb_utils import save_to_duckdb

def run_all(dataset_path: str,
            outputs_dir: str,
            *,
            save_duckdb: bool = True,
            duckdb_path: str | None = None):
    """
    Pipeline:
    1. Preprocess raw data
    2. Build contacts, UTM, and events tables
    3. Roll up user-level engagement metrics
    4. Compute per-source KPIs + event category distribution
    5. Save outputs (CSVs + charts)
    6. (Optional) Persist the model into a DuckDB file
    7. Return artifact paths
    """

    # 1) Loading & preprocessing
    df = pd.read_csv(dataset_path)
    df = clean_dataframe(df)
    df = parse_datetimes(df)
    df = drop_impossible_dates(df)

    # 2) Building model tables
    contacts = build_contacts(df)
    utm = extract_utm(df)
    events = build_events(df)
    users, events_after_acq = rollup_users(contacts, utm, events)

    # 3) Computing metrics
    metrics = metrics_per_utm(users)
    cat_mix = event_category_mix(events_after_acq, utm)

    # 4) Saving CSV outputs
    outdir = Path(outputs_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(outdir / "per_utm_metrics.csv", index=False)
    users.to_csv(outdir / "users_table.csv", index=False)
    cat_mix.to_csv(outdir / "event_category_mix.csv", index=False)

    # 5) Saving charts
    chart_bar(metrics.sort_values("acquisition_volume", ascending=False),
              x="utm_source", y="acquisition_volume",
              title="Acquisition Volume by UTM Source",
              xlab="UTM Source", ylab="Users acquired",
              outpath=outdir / "acquisition_volume_by_utm.png")

    chart_bar(metrics.sort_values("acquisition_volume", ascending=False),
              x="utm_source", y="engagement_rate",
              title="Engagement Rate by UTM Source",
              xlab="UTM Source", ylab="Engagement rate",
              outpath=outdir / "engagement_rate_by_utm.png")

    chart_bar(metrics.sort_values("acquisition_volume", ascending=False),
              x="utm_source", y="retention_rate",
              title="Retention Rate by UTM Source",
              xlab="UTM Source", ylab="Retention rate",
              outpath=outdir / "retention_rate_by_utm.png")

    # 6) Saving the data model to a dedicated DuckDB folder
    db_file = None
    if save_duckdb:
        duckdb_dir = Path(outputs_dir) / "duckdb"
        duckdb_dir.mkdir(parents=True, exist_ok=True)
        db_file = duckdb_path or str(duckdb_dir / "attribution.duckdb")

        save_to_duckdb(
            users=users,
            contacts=contacts,
            utm=utm,
            events=events,
            metrics=metrics,
            cat_mix=cat_mix,
            db_path=db_file
        )

    # 7) Returning artifacts (include dedicated DuckDB path)
    return {
        "duckdb_file": db_file,
        "duckdb_dir": str(Path(outputs_dir) / "duckdb"),
        "metrics_csv": str(outdir / "per_utm_metrics.csv"),
        "users_csv": str(outdir / "users_table.csv"),
        "cat_mix_csv": str(outdir / "event_category_mix.csv"),
        "acq_png": str(outdir / "acquisition_volume_by_utm.png"),
        "eng_png": str(outdir / "engagement_rate_by_utm.png"),
        "ret_png": str(outdir / "retention_rate_by_utm.png"),
    }

