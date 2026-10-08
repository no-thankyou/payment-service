import logging


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs every request with its full URL at INFO; webhook URLs may embed secrets/tokens.
    logging.getLogger("httpx").setLevel(logging.WARNING)
