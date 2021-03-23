import logging
import os
import sys
from pathlib import Path

LOG_MAP = {}
LOGGING_FORMATTER = '%(asctime)s - %(processName)s:%(threadName)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'


def getLogging(name, path="", level=logging.DEBUG) -> logging:
    if path == "":
        path = str(Path.home())

    logger = logging.getLogger(name)
    if LOG_MAP.get(name, None) is None:
        LOG_MAP[name] = name
        logger.setLevel(level)  # Log等级总开关
        formatter = logging.Formatter(LOGGING_FORMATTER)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)  # 输出到console的log等级的开关
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        os.makedirs(f"{path}/logs", exist_ok=True)
        logfile = f'{path}/logs/{name}.log'
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger
