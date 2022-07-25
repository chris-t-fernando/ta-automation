import logging
from cloudwatch import cloudwatch

logger = logging.getLogger("my_logger")
formatter = logging.Formatter("%(asctime)s : %(levelname)s - %(message)s")

handler = cloudwatch.CloudwatchHandler(log_group="tabot", log_stream="banana1")

# Pass the formater to the handler
handler.setFormatter(formatter)
# Set the level
logger.setLevel(logging.DEBUG)
# Add the handler to the logger
logger.addHandler(handler)


start = 200
end = start + 10
for i in range(start, end):
    logger.debug(f"some message {i}")
