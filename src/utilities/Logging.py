# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Logging utility for 
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""
import logging
from typing     import Any
from environs   import Env

env = Env()



class Logging:
    """Utility class for centralized application logging."""
    # Set default logging level and format from environment variables or use defaults    
    _DEFAULT_LEVEL = "INFO"
    _DEFAULT_FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"

    _level_name = env.str("OFTL_LOG_LEVEL", _DEFAULT_LEVEL).upper()
    _log_format = env.str("OFTL_LOG_FORMAT", _DEFAULT_FORMAT)    

    _log_level = getattr(logging, _level_name, logging.INFO)

    logging.basicConfig(level=_log_level, format=_log_format)
    _logger = logging.getLogger("OFTL")

    @classmethod
    def info(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.info(message, *args, **kwargs)

    @classmethod
    def warning(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.warning(message, *args, **kwargs)

    @classmethod
    def error(cls, message: str, *args: Any, **kwargs: Any) -> None:
        cls._logger.error(message, *args, **kwargs)
