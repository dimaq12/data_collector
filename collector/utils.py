import pandas as pd

def normalize_timestamp(ts, interval: str) -> pd.Timestamp:
    """
    Приводит входной ts к pd.Timestamp с округлением вниз по заданному интервалу.
    Поддерживает input в миллисекундах (по умолчанию — под Binance).
    """
    if isinstance(ts, (int, float)):
        # Binance всегда отдаёт в миллисекундах
        ts = pd.to_datetime(ts, unit='ms', utc=True)
    elif isinstance(ts, str):
        ts = pd.to_datetime(ts, utc=True)
    elif isinstance(ts, pd.Timestamp):
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    elif hasattr(ts, 'timestamp'):
        ts = pd.Timestamp(ts).tz_localize("UTC") if ts.tzinfo is None else pd.Timestamp(ts).tz_convert("UTC")
    else:
        raise ValueError(f"Unsupported timestamp type: {type(ts)}")

    # Округляем вниз по интервалу
    if interval.endswith('m'):
        step = pd.Timedelta(minutes=int(interval[:-1]))
    elif interval.endswith('h'):
        step = pd.Timedelta(hours=int(interval[:-1]))
    else:
        raise ValueError(f"Unsupported interval format: {interval}")

    return ts.floor(step)
