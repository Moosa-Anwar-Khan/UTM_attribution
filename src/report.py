from pathlib import Path
import pandas as pd
from .preprocess import clean_dataframe, parse_datetimes, drop_impossible_dates
from .modeling import build_contacts, extract_utm, build_events, rollup_users
from .metrics import metrics_per_utm, event_category_mix
from .visuals import chart_bar


def run_all(dataset_path: str, outputs_dir: str):
    """
    Orchestrates the entire pipeline:
    1. Preprocess raw data
    2. Build contacts, UTM, and events tables
    3. Roll up user-level engagement metrics
    4. Compute per-source KPIs + event category distribution
    5. Save outputs (CSVs + charts)
    6. Return artifact paths for convenience
    """

    # 1. Loading & preprocessing
    df = pd.read_csv(dataset_path)      
    df = clean_dataframe(df)            
    df = parse_datetimes(df)            
    df = drop_impossible_dates(df)      

    # 2. Building model tables
    contacts = build_contacts(df)       
    utm = extract_utm(df)               
    events = build_events(df)           
    users, events_after_acq = rollup_users(contacts, utm, events)
    # users = per-user rollups (with engagement flags)
    # events_after_acq = enriched events with "is_after_acq" flag

    # 3. Computing metrics
    metrics = metrics_per_utm(users)          
    cat_mix = event_category_mix(events_after_acq, utm)  

    # 4. Saving CSV outputs
    outdir = Path(outputs_dir)
    outdir.mkdir(parents=True, exist_ok=True)   # create outputs/ folder if missing
    metrics.to_csv(outdir / "per_utm_metrics.csv", index=False)
    users.to_csv(outdir / "users_table.csv", index=False)
    cat_mix.to_csv(outdir / "event_category_mix.csv", index=False)

    # 5. Saving charts
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

    # 6. Providing the resulting file locations.
    return {
        "metrics_csv": str(outdir / "per_utm_metrics.csv"),
        "users_csv": str(outdir / "users_table.csv"),
        "cat_mix_csv": str(outdir / "event_category_mix.csv"),
        "acq_png": str(outdir / "acquisition_volume_by_utm.png"),
        "eng_png": str(outdir / "engagement_rate_by_utm.png"),
        "ret_png": str(outdir / "retention_rate_by_utm.png"),
    }
