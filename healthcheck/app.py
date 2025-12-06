import connexion
from connexion import NoContent
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import json
import requests
import os
from connexion.middleware import MiddlewarePosition 
from starlette.middleware.cors import CORSMiddleware

# Load config files
with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Status datastore
DATA_FILE = app_config['datastore']['filename']

def poll_services():
    """Poll receiver, storage, processing, analyzer"""
    logger.info("Polling services...")
    statuses = {}

    for service in ["receiver", "storage", "processing", "analyzer"]:
        url = app_config["services"][service]
        try:
            r = requests.get(url, timeout=5)
            statuses[service] = "Running" if r.status_code == 200 else "Down"
        except:
            statuses[service] = "Down"

    statuses["last_update"] = datetime.utcnow().isoformat()

    with open(DATA_FILE, "w") as f:
        json.dump(statuses, f)

    logger.info(f"Updated service health: {statuses}")


def get_status():
    """Return latest stored statuses"""
    if not os.path.exists(DATA_FILE):
        return {"message": "No status data yet"}, 404

    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    return data, 200

app = connexion.FlaskApp(__name__, specification_dir='')
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
    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_services, 'interval', seconds=app_config['scheduler']['period_sec'])
    scheduler.start()

    app.run(port=8120, host="0.0.0.0")
