import logging
import sys

def setup_logger(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("app")
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
