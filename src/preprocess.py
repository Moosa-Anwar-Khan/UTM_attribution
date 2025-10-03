import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning:
    - Standardize column names (strip)
    - Trim string values
    - Handle missing values ('', 'nan', 'None' -> None)
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(['', 'nan', 'None']), col] = None
    return df

def parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["Contact Create date", "Contact Update date", "Events Create date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def drop_impossible_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional: guard-rail for obviously bad timestamps (before 2000 or far future).
    """
    df = df.copy()
    low, high = pd.Timestamp("2000-01-01"), pd.Timestamp("2100-01-01")
    for c in ["Contact Create date", "Contact Update date", "Events Create date"]:
        if c in df.columns:
            mask = (df[c].notna()) & ((df[c] < low) | (df[c] > high))
            df.loc[mask, c] = pd.NaT
    return df
