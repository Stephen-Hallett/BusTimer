import functools
import logging
import time
from collections.abc import Callable

from ..config import settings

# Yoinked straight from https://github.com/Estanz0/CVGenerator/blob/main/backend/app/util.py


# Logging
class MyLogger:
    def __init__(self) -> None:
        logging.basicConfig(level=logging.DEBUG if settings.debug else logging.INFO)

    def get_logger(self, name: str | None = None) -> logging.Logger:
        return logging.getLogger(name)


def get_default_logger() -> logging.Logger:
    return MyLogger().get_logger()


def log(
    _func: Callable | None = None, *, my_logger: MyLogger | logging.Logger = None
) -> str:
    def decorator_log(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> str:  # noqa: ANN002, ANN003
            logger = get_default_logger()
            try:
                if my_logger is None:
                    first_args = next(
                        iter(args), None
                    )  # capture first arg to check for `self`
                    logger_params = [  # does kwargs have any logger
                        x
                        for x in kwargs.values()
                        if isinstance(x, logging.Logger | MyLogger)
                    ] + [  # # does args have any logger
                        x for x in args if isinstance(x, logging.Logger | MyLogger)
                    ]
                    if hasattr(first_args, "__dict__"):  # is first argument `self`
                        logger_params = logger_params + [
                            x
                            # does class (dict) members have any logger
                            for x in first_args.__dict__.values()
                            if isinstance(x, logging.Logger | MyLogger)
                        ]
                    h_logger = next(
                        iter(logger_params), MyLogger()
                    )  # get the next/first/default logger
                else:
                    h_logger = my_logger  # logger is passed explicitly to the decorator

                logger = (
                    h_logger.get_logger(func.__name__)
                    if isinstance(h_logger, MyLogger)
                    else h_logger
                )

                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                signature = ", ".join(args_repr + kwargs_repr)
                if settings.debug:
                    logger.debug(
                        f"function {func.__name__} called with args {signature}"
                    )
                else:
                    logger.info(f"function {func.__name__} called")
            except Exception as e:
                raise e

            try:
                start_time = time.time()

                result = func(*args, **kwargs)

                end_time = time.time()
                elapsed_time = end_time - start_time
                logger.info(
                    f"function {func.__name__} completed in {elapsed_time:.4f} seconds"
                )
                return result
            except Exception as e:
                logger.exception(
                    f"Exception raised in {func.__name__}. exception: {e!s}"
                )
                raise e

        return wrapper

    if _func is None:
        return decorator_log
    return decorator_log(_func)
