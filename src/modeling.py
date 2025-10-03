import pandas as pd

# Building a CONTACTS table
def build_contacts(df: pd.DataFrame) -> pd.DataFrame:
    contacts = (
        # Selecting only the contact-related columns
        df[["Contact ID","Contact Create date","Contact Update date"]]

        # Ensuring each contact appears only once
        .drop_duplicates(subset=["Contact ID"])

        # Renaming columns to cleaner names
        .rename(columns={"Contact Create date":"contact_created_at",
                         "Contact Update date":"contact_updated_at"})
    )
    return contacts


# Building a UTM table (acquisition source)
def extract_utm(df: pd.DataFrame) -> pd.DataFrame:
    utm = (
        # Keeping only rows where the form field was "utm_source"
        df[df["Fields Title"]=="utm_source"][["Contact ID","Field Value"]]

        # Dropping rows where the UTM value is missing
        .dropna()

        # Ensuring only one UTM value per contact
        .drop_duplicates(subset=["Contact ID"])

        # Renaming "Field Value" to "utm_source"
        .rename(columns={"Field Value":"utm_source"})
    )
    # Normalizing text: remove spaces, lowercase for consistency
    utm["utm_source"] = utm["utm_source"].str.strip().str.lower()
    return utm


# Building an EVENTS table
def build_events(df: pd.DataFrame) -> pd.DataFrame:
    events = (
        # Keeping only rows that represent actual events (have an Events ID)
        df[df["Events ID"].notna()][
            ["Contact ID","Events ID","Events Category","Events Create date","Events Hash"]
        ]

        # Renaming columns to cleaner names
        .rename(columns={
            "Events ID":"event_id",
            "Events Category":"event_category",
            "Events Create date":"event_created_at",
            "Events Hash":"event_hash"
        })
    )
    return events


# Rolling up user-level metrics
def rollup_users(contacts: pd.DataFrame, utm: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    # Starting with contacts joined to their UTM source
    users = contacts.merge(utm, on="Contact ID", how="left")

    # labelling as "unknown" If no UTM is available
    users["utm_source"] = users["utm_source"].fillna("unknown")

    # Joining events to contact signup dates to compare event timing vs acquisition
    ea = events.merge(contacts[["Contact ID","contact_created_at"]], on="Contact ID", how="left") # ea => Events after Acquisition join
    
    # Flagging whether the event happened after the user signed up (true engagement)
    ea["is_after_acq"] = ea["event_created_at"] >= ea["contact_created_at"]

    # Grouping events per contact and computing engagement summary metrics
    gr = (
        ea.groupby("Contact ID")
        .agg(
            total_events=("event_id","nunique"),                       # number of unique events overall
            events_after_acq=("is_after_acq","sum"),                   # how many events were after acquisition
            first_event_at=("event_created_at","min"),                 # first event timestamp
            distinct_event_days=("event_created_at", lambda x: x.dt.date.nunique())  # unique days with activity
        )
        .reset_index()
    )

    # Merging the rollup metrics back into the user table
    users = users.merge(gr, on="Contact ID", how="left")

    # Replacing NaN values with 0 for users with no events, and casting to integers
    for c in ["total_events","events_after_acq","distinct_event_days"]:
        if c in users.columns:
            users[c] = users[c].fillna(0).astype(int)

    # Defining funnel flags
    users["engaged"] = users.get("events_after_acq", 0) >= 1     # engaged if at least 1 event after signup
    users["retained"] = users.get("distinct_event_days", 0) >= 2 # retained if active on at least 2 days

    # Returning the per-user dataset and also ea
    return users, ea
