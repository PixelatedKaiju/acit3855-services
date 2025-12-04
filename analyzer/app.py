import connexion
import yaml
import logging
import logging.config
import json
import httpx
from datetime import datetime
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')


def get_reading_stats():
    """
    Return total counts of search and purchase readings from STORAGE.
    """
    logger.info("Request for analyzer statistics received")

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

    # Use a very early timestamp to read ALL data
    start_ts = "2000-01-01T00:00:00.000Z"

    # GET SEARCH READINGS
    search_url = app_config['eventstores']['search']['url']
    search_response = httpx.get(
        search_url,
        params={"start_timestamp": start_ts, "end_timestamp": now}
    )

    num_search = 0
    if search_response.status_code == 200:
        search_readings = search_response.json()
        num_search = len(search_readings)
        logger.info(f"Retrieved {num_search} search readings")
    else:
        logger.error("Failed to retrieve search readings")

    # GET PURCHASE READINGS 
    purchase_url = app_config['eventstores']['purchase']['url']
    purchase_response = httpx.get(
        purchase_url,
        params={"start_timestamp": start_ts, "end_timestamp": now}
    )

    num_purchase = 0
    if purchase_response.status_code == 200:
        purchase_readings = purchase_response.json()
        num_purchase = len(purchase_readings)
        logger.info(f"Retrieved {num_purchase} purchase readings")
    else:
        logger.error("Failed to retrieve purchase readings")

    stats = {
        "num_search_readings": num_search,
        "num_purchase_readings": num_purchase
    }

    logger.info(f"Stats returned: {stats}")

    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir='')

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8110, host="0.0.0.0")
