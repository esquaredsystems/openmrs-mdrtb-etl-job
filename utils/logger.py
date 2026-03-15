from loguru import logger

logger.add(
    "etl.log",
    rotation="10 MB",
    retention="7 days",
    level="INFO"
)

def info(message):
    logger.info(message)

def debug(message):
    logger.debug(message)

def error(message):
    logger.error(message)

def warning(message):
    logger.warning(message)

__all__ = ["info", "debug", "error", "warning"]