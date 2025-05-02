import logging
import os
from datetime import datetime


def setup_logging() -> None:
    """
    Sets up the logging configuration for the application.

    This function creates a directory for log files, sets up a logging configuration that outputs logs to a file
    and the console, and includes timestamps and log levels in the log messages.
    """
    print("Setting up logging...")
    # Create a directory named 'logs' in the current working directory to store log files
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)

    # Generate a log file name with a timestamp to ensure unique filenames for each run
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(logs_dir, f"log_{current_time}.log")

    # Set up the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # File handler
    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)
    except Exception as e:
        print(f"Failed to create log file: {e}")
        return

    # Remove existing handlers and add new ones
    if root_logger.handlers:
        root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Suppress debug logs from specific third-party libraries
    suppressed_libraries = [
        "requests",
        "urllib3",
        "PIL",
        "concurrent.futures",
        "selenium.webdriver.remote.remote_connection",
    ]
    for library in suppressed_libraries:
        logging.getLogger(library).setLevel(logging.WARNING)

    logging.info("Logging set up successfully. Logs will be written to: %s", log_file)
