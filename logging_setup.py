# -*- coding: utf-8 -*-
from __future__ import annotations

from app_config import EXPORT_DIR, HERO_SMS_LOG_FILE, LOG_FILE


def setup_hero_sms_logging():
    import logging

    logger = logging.getLogger("hero_sms")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = logging.FileHandler(HERO_SMS_LOG_FILE, mode="w", encoding="utf-8")
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def setup_logging():
    import logging
    import sys

    EXPORT_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
    # Reduce extremely chatty Selenium/HTTP logs; they slow down proxy-heavy runs
    # and make app.log too large without adding actionable value.
    for noisy in (
        "selenium",
        "selenium.webdriver",
        "selenium.webdriver.remote.remote_connection",
        "urllib3",
        "urllib3.connectionpool",
        "httpcore",
        "httpx",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    setup_hero_sms_logging()
    return logging.getLogger(__name__)


def log_exception(logger=None, msg=""):
    import logging

    log = logger or logging.getLogger(__name__)
    log.exception(msg or "Lỗi")
