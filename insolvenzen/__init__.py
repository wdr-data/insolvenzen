import os
import sys

from loguru import logger

DEBUG = False
if os.environ.get("DEBUG") == "True":
    DEBUG = True

if DEBUG:
    lvl = "TRACE"
else:
    lvl = "DEBUG"

fmt = (
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

# AWS adds its own timestamp to log lines
if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
    fmt = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | " + fmt

logger.remove()  # Remove default logger
logger.add(sys.stderr, level=lvl, format=fmt, diagnose=DEBUG)
logger.info("Logging setup complete.")
