# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Service Template / starter code for PayTrace SCA Service build on fastapi.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""

from fastapi                import FastAPI
from routes.Routes          import Routes
from utilities.Logging      import Logging
from utilities.ConfigLoader import ConfigLoader 
from utilities.DBHelper     import DBHelper
from utilities.HeaderValidationMiddleware import HeaderValidationMiddleware
from contextlib             import asynccontextmanager
from uvicorn.config         import LOGGING_CONFIG


import uvicorn
import sys


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:        
        result = DBHelper.initialize_connection()        
        if not result:
            Logging.info("Database connection is not established.")
            Logging.error("Failed to initialize database connection during startup.")
            raise RuntimeError("Database initialization failed. Service startup aborted.")
    except Exception as ex:
        Logging.info("Database connection is not established.")
        Logging.error(f"Database startup error: {ex}")
        raise
    yield
    DBHelper.dispose_connection()


app         = FastAPI(lifespan=lifespan)
routes      = Routes()
app.add_middleware(HeaderValidationMiddleware)

# Initializing the FastAPI app and loading routes from the Routes class.
app.include_router(routes.router)
app.include_router(routes.public_router)
# END;

# Default variables
_DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
_DEFAULT_LOG_LEVEL  = "INFO"
_DEFAULT_HOST       = "0.0.0.0"
_DEFAULT_PORT       = 8081
# END;


def displayBanner():
    Logging.info("===============================================")
    Logging.info("Starting PayTrace SCA Service")
    Logging.info(f"Version: {ConfigLoader.get('OFTL_SCA_VERSION', 'N/A')}")
    Logging.info(f"Context Root: {ConfigLoader.get('OFTL_SCA_CONTEXT_ROOT', 'N/A')}")
    Logging.info(f"Host: {ConfigLoader.get('OFTL_SCA_HOST', _DEFAULT_HOST)}")
    Logging.info(f"Port: {ConfigLoader.get('OFTL_SCA_PORT', _DEFAULT_PORT)}")
    Logging.info(f"Log Level: {ConfigLoader.get('OFTL_LOG_LEVEL', _DEFAULT_LOG_LEVEL)}")
    Logging.info(f"Database: {ConfigLoader.get('OFTL_POSTGRESDB_NAME', "N/A")}")
    Logging.info(f"Database Host: {ConfigLoader.get('OFTL_POSTGRESDB_HOST', "N/A")}")
    Logging.info("===============================================")

    pass


if __name__ == "__main__":
    try:
        displayBanner()
        # Setting log cofig and format
        
        log_config = LOGGING_CONFIG
        log_config["formatters"]["default"]["fmt"] = ConfigLoader.get("OFTL_LOG_FORMAT", _DEFAULT_LOG_FORMAT)
        log_config["handlers"]["default"]["level"] = ConfigLoader.get("OFTL_LOG_LEVEL", _DEFAULT_LOG_LEVEL)
        # END;
        uvicorn.run(app, 
                    host=ConfigLoader.get("OFTL_SCA_HOST", _DEFAULT_HOST),
                    port=int(ConfigLoader.get("OFTL_SCA_PORT", _DEFAULT_PORT)),
                    log_config=log_config,
                )
        
    except Exception as e:
        Logging.error(f"Error starting SCA Service")  
        Logging.error(str(e))
        sys.exit(91)
