# logger.py
import logging
import os
from logging.handlers import RotatingFileHandler

# Create logs directory if it does not exist
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

log_file_path = os.path.join(logs_dir, 'tvfactory_collector.log')

# Configure logging
logger = logging.getLogger("tvfactory_collector")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file_path, maxBytes=10000000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
