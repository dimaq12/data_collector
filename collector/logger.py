import os
import json
import logging
from config import LOG_LEVEL, LOG_FORMAT


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        # include any additional attributes from "extra"
        for key, value in record.__dict__.items():
            if key not in ("name", "msg", "args", "levelname", "levelno", "pathname",
                           "filename", "module", "exc_info", "exc_text", "stack_info",
                           "lineno", "funcName", "created", "msecs", "relativeCreated",
                           "thread", "threadName", "processName", "process", "message"):
                log_record[key] = value
        return json.dumps(log_record)


def setup_logging() -> None:
    """Configure root logging according to LOG_LEVEL and LOG_FORMAT."""
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    handler = logging.StreamHandler()
    if LOG_FORMAT.lower() == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
        )
    handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[handler])
