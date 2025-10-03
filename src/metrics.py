import pandas as pd
import numpy as np

# Function: metrics_per_utm
def metrics_per_utm(users: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        users.groupby("utm_source")   # grouping users by their acquisition source
        .agg(
            acquisition_volume=("Contact ID","nunique"),             # how many unique users came from this source
            engaged_users=("engaged","sum"),                         # how many of those became engaged (True counts as 1)
            retention_users=("retained","sum"),                      # how many of those became retained (True counts as 1)
            avg_events_per_user=("total_events",                     # average number of events per user
                                 lambda x: float(np.mean(x)) if len(x)>0 else 0.0)
        ).reset_index()
    )

    # Calculating engagement rate = % of acquired users who engaged
    metrics["engagement_rate"] = (metrics["engaged_users"] / metrics["acquisition_volume"]).round(3)

    # Calculating retention rate = % of acquired users who retained
    metrics["retention_rate"] = (metrics["retention_users"] / metrics["acquisition_volume"]).round(3)

    # Returning metrics sorted by acquisition volume (biggest sources first)
    return metrics.sort_values("acquisition_volume", ascending=False).reset_index(drop=True)


# Function: event_category_mix
def event_category_mix(events_after_acq: pd.DataFrame, utm: pd.DataFrame) -> pd.DataFrame:
    # Merging events with their UTM source
    e = events_after_acq.merge(utm, on="Contact ID", how="left")
    e["utm_source"] = e["utm_source"].fillna("unknown")  # if no source, mark as 'unknown'

    # Counting number of events per (utm_source, event_category)
    cat_mix = (e[e["is_after_acq"]]    # only including events that happened after acquisition
               .groupby(["utm_source","event_category"])
               .size()
               .reset_index(name="events"))

    # Getting total events per UTM source
    totals = cat_mix.groupby("utm_source")["events"].sum().reset_index(name="total_events")

    # Merging totals back into the per-category table
    cat_mix = cat_mix.merge(totals, on="utm_source", how="left")

    # Calculating share (%) of each event category per source
    cat_mix["share"] = (cat_mix["events"]/cat_mix["total_events"]).round(3)

    # Returning table: for each UTM source, event categories and their proportion
    return cat_mix
