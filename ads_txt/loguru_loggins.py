import sys
from loguru import logger

sys.stdout.reconfigure(encoding='utf-8')


# Configure logger
logger.remove()  # Remove the default logger
logger.add(sys.stdout, level="DEBUG", format="{time} {level} {file.name}:{function}:{line} - {message}", backtrace=True, diagnose=True)
logger.add("Log_files/app.log", retention="10 days", level="INFO", format="{time} {level} {file.name}:{function}:{line} - {message}", backtrace=True, diagnose=True)

# Function to get the logger
def get_logger():
    return logger

# Function to log a separator line
def log_separator():
    separator_line = "\n" + ("-" * 50)
    logger.info(separator_line * 2)  # Add a separator line with dashes

# Log a separator line at the start of the run
log_separator()

logger.info("Logger setup is complete!!!")