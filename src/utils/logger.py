import logging
import os

def setup_logger(name: str, config: dict = None) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    # Default values
    level_str = "INFO"
    enable_file_logging = False
    file_path = "logs/app.log"

    if config:
        level_str = config.get("level", level_str)
        enable_file_logging = config.get("enable_file_logging", enable_file_logging)
        file_path = config.get("file_path", file_path)

    level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if enable_file_logging:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        fh = logging.FileHandler(file_path)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
