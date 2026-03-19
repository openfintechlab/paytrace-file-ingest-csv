# -*- coding: utf-8 -*-
"""
PayTrace CSV file watcher entrypoint.
"""

from __future__ import annotations

import asyncio
import sys

try:
    from utilities.ConfigLoader import ConfigLoader
    from utilities.FileWatcher import FileWatcherAgent
    from utilities.Logging import Logging
    from utilities.RabbitMQHelper import RabbitMQConnectionError, RabbitMQHelper, RabbitMQShutdownRequested
except ModuleNotFoundError:
    from src.utilities.ConfigLoader import ConfigLoader
    from src.utilities.FileWatcher import FileWatcherAgent
    from src.utilities.Logging import Logging
    from src.utilities.RabbitMQHelper import RabbitMQConnectionError, RabbitMQHelper, RabbitMQShutdownRequested

_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_ROOT_DIR = "./fwcsv"


def display_banner() -> None:
    Logging.info("===============================================")
    Logging.info("Starting PayTrace File Watcher")
    Logging.info("Version: %s", ConfigLoader.get("OFTL_SCA_VERSION", "N/A"))
    Logging.info("Root Dir: %s", ConfigLoader.get("OFTL_FWCSV_ROOTDIR", _DEFAULT_ROOT_DIR))
    Logging.info("Log Level: %s", ConfigLoader.get("OFTL_LOG_LEVEL", _DEFAULT_LOG_LEVEL))
    Logging.info("Database: %s", ConfigLoader.get("OFTL_POSTGRESDB_NAME", "N/A"))
    Logging.info("Database Host: %s", ConfigLoader.get("OFTL_POSTGRESDB_HOST", "N/A"))
    Logging.info("===============================================")


async def _run() -> None:
    RabbitMQHelper.initialize_connection()
    Logging.info("RabbitMQ startup connection established.")
    agent = FileWatcherAgent()
    await agent.run_forever()


if __name__ == "__main__":
    try:
        display_banner()
        Logging.info("Application starting...")
        asyncio.run(_run())
    except KeyboardInterrupt:
        Logging.warning("Shutdown requested by user.")
        sys.exit(0)
    except RabbitMQShutdownRequested as exc:
        Logging.error("Error starting file watcher service")
        Logging.error(str(exc))
        sys.exit(99)
    except RabbitMQConnectionError as exc:
        Logging.error("Error starting file watcher service")
        Logging.error(str(exc))
        sys.exit(99)
    except Exception as exc:
        Logging.error("Error starting file watcher service")
        Logging.error(str(exc))
        sys.exit(91)
