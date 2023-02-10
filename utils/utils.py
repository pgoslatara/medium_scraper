import logging

def set_logging_options() -> None:
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )

    logging.getLogger("py4j").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)