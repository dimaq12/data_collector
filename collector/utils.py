import pandas as pd

def ensure_utc_aware(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    else:
        return ts.tz_convert("UTC")