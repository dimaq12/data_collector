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


def normalize_since(ts: pd.Timestamp, interval: str) -> pd.Timestamp:
    if interval.endswith("m"):
        step = int(interval[:-1])
        minute = (ts.minute // step) * step
        return ts.replace(minute=minute, second=0, microsecond=0)
    elif interval.endswith("h"):
        step = int(interval[:-1])
        hour = (ts.hour // step) * step
        return ts.replace(hour=hour, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unsupported interval: {interval}")

def normalize_until(ts: pd.Timestamp, interval: str) -> pd.Timestamp:
    ts = normalize_since(ts, interval)
    if interval.endswith("m"):
        step = int(interval[:-1])
        return ts - pd.Timedelta(minutes=step)
    elif interval.endswith("h"):
        step = int(interval[:-1])
        return ts - pd.Timedelta(hours=step)
    else:
        raise ValueError(f"Unsupported interval: {interval}")
