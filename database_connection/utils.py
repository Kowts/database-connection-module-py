import time
import logging
from functools import wraps

def retry(max_retries=3, delay=1, backoff=2, exceptions=(Exception,), logger=None):
    """
    Decorator for retrying a function with exponential backoff.

    This decorator will retry the decorated function if it raises any of the specified exceptions.
    The number of retries, initial delay, and backoff factor can be configured.
    The delay between retries will increase exponentially based on the backoff factor.

    Args:
        max_retries (int): Maximum number of retry attempts. Default is 3.
        delay (int or float): Initial delay between retries in seconds. Default is 1.
        backoff (int or float): Factor by which the delay is multiplied after each retry. Default is 2.
        exceptions (tuple): Tuple of exception classes to catch and retry on. Default is (Exception,).
        logger (logging.Logger): Logger instance for logging. Default is None, which will use the root logger.

    Returns:
        function: A wrapped version of the original function with retry logic.

    Raises:
        Exception: Reraise the last exception encountered if max_retries is exceeded.

    Example:
        @retry(max_retries=5, delay=2, backoff=1.5, exceptions=(ConnectionError,))
        def connect_to_database(config):
            # Attempt to connect to the database
            pass
    """
    def decorator(func):
        @wraps(func)  # Preserves the original function name and docstring
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            log = logger or logging  # Use provided logger or root logger

            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    log.error(f"Attempt {attempt} failed with error: {e}. Retrying in {current_delay} seconds...")

                    if attempt >= max_retries:
                        log.error(f"Max retries exceeded for function {func.__name__}")
                        raise

                    time.sleep(current_delay)
                    current_delay *= backoff  # Exponentially increase the delay

        return wrapper
    return decorator
