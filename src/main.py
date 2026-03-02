# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Service Template / starter code for PayTrace SCA Service build on fastapi.
Reference: https://github.com/orgs/openfintechlab/projects/3/views/7
"""


from utilities.Logging      import Logging
from utilities.ConfigLoader import ConfigLoader 
from utilities.DBHelper     import DBHelper

import sys





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
        Logging.info("Application starting...")
        
        
    except Exception as e:
        Logging.error(f"Error starting SCA Service")  
        Logging.error(str(e))
        sys.exit(91)
