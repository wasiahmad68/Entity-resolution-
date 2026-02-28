import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Returns a centrally configured logger instance to ensure standard
    formatting and output collection across all engine modules.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console Handler outputs to standard stdout natively
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    return logger
