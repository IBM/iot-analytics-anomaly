import os
import json
import logging
from dotenv import load_dotenv
from os.path import join, dirname


def set_log_level(logger_level):
    logging.getLogger().setLevel(logger_level)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logger_level)

    # create formatter
    formatter = logging.Formatter('\n%(asctime)s - %(name)s - %(levelname)s - \n%(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logging.getLogger().addHandler(ch)


# Configure Logging Default
set_log_level(logging.INFO)

# If on bluemix load env differently
# Load Environment variables set via VCAP variables in Bluemix
#if 'VCAP_SERVICES' in os.environ:
    # print("On Bluemix...")

# Check for existance of .env file
env_path = join(dirname(__file__), '.env')

# Load .env file into os.environ
load_dotenv(env_path)

# ===================
# Logging Settings
# ===================
try:
    # API Key for bot analytics
    LOG_LEVEL = os.environ.get("LOG_LEVEL").upper()
    log_level_str = LOG_LEVEL

    if LOG_LEVEL == 'INFO':
        LOG_LEVEL = logging.INFO
    elif LOG_LEVEL == 'DEBUG':
        LOG_LEVEL = logging.DEBUG
    elif LOG_LEVEL == 'WARNING':
        LOG_LEVEL = logging.WARNING
    elif LOG_LEVEL == 'ERROR':
        LOG_LEVEL = logging.ERROR
    else:
        LOG_LEVEL = logging.WARNING

    logging.info("Logging Set To: " + log_level_str)

    if LOG_LEVEL != logging.WARNING:
        set_log_level(LOG_LEVEL)
except Exception as ex:
    template = 'Error: {0} Problem reading Logging Level string from environment variables. Logging set to WARNING LEVEL. Arguments: \n{1!r}'
    message = template.format(type(ex).__name__, ex.args)
    logging.warning(message)

# =======================================
# Building Insights Assistant Credentials
# =======================================
try:
    #  Credentials
    print("Loading WML Environment Variables")
    WATSON_ML_ENDPOINT= os.environ.get("WATSON_ML_ENDPOINT")
    WATSON_ML_DEPLOYMENT_ID = os.environ.get("WATSON_ML_DEPLOYMENT_ID")
    WATSON_ML_INSTANCE_ID = os.environ.get("WATSON_ML_INSTANCE_ID")
    WATSON_ML_APIKEY= os.environ.get("WATSON_ML_APIKEY")
    INPUT_COLUMNS = os.environ.get("INPUT_COLUMNS")
    ENTITY_NAME = os.environ.get("ENTITY_NAME")
    # MODEL_OUTPUT_COLUMNS = os.environ.get("MODEL_OUTPUT_COLUMNS") # for multivariate output
    # WATSON_ML_MODEL_ID= os.environ.get("WATSON_ML_MODEL_ID")
    # IAM_UID= os.environ.get("IAM_UID")
    # IAM_PASSWORD= os.environ.get("IAM_PASSWORD")

    print("Environment Variables Loaded Successfully")

except Exception as ex:
    print("ERROR: Missing Required Environment Variables")
