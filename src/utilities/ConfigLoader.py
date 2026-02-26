# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Configuration loader utility for OFTL-prefixed variables.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""

import os
import re
from typing import Any, ClassVar

from environs import Env


class ConfigLoader:
    """Load OFTL configuration from environment variables and .env file."""

    _KEY_PATTERN = re.compile(r"^OFTL_[A-Z0-9]+_[A-Z0-9_]+(?:_SECRET)?$")
    _env = Env()
    configurations: ClassVar[dict[str, str]] = {}

    @classmethod
    def decrypt(cls, value: str) -> str:
        """
        Decrypt secrets from environment values.
        Replace this implementation when real decryption is available.
        """
        return value

    @classmethod
    def _is_valid_key(cls, key: str) -> bool:
        return bool(cls._KEY_PATTERN.fullmatch(key))

    @classmethod
    def load_configurations(cls) -> dict[str, str]:
        """Load validated OFTL variables from process env and .env into a dictionary."""
        cls._env.read_env()  # Loads .env values (if present) into os.environ.
        loaded: dict[str, str] = {}

        for key, value in os.environ.items():
            if not key.startswith("OFTL_"):
                continue
            if not cls._is_valid_key(key):
                continue

            parsed_value = cls.decrypt(value) if key.endswith("_SECRET") else value
            loaded[key] = parsed_value

        cls.configurations = loaded
        return cls.configurations

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        if not cls.configurations:
            cls.load_configurations()
        return cls.configurations.get(key, default)


ConfigLoader.load_configurations()
