import connexion
from connexion import NoContent
from datetime import datetime
import time
import yaml
import logging
import logging.config
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException  
import json
import random  


with open('/config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('/config/log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client():
                if self.make_consumer():
                    break
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        if self.client is not None:
            return True

        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

    def make_producer(self):
        if self.client is None:
            self.make_client()

        try:
            topic = self.client.topics[self.topic.encode('utf-8')] 
            producer = topic.get_sync_producer()
            logger.info("Kafka producer created!")
            return producer
        except KafkaException as e:
            logger.error(f"Kafka producer error: {e}")
            return None

    def messages(self):
        if self.consumer is None:
            self.connect()

        while True:
            try:
                for msg in self.consumer:
                    yield msg

            except KafkaException as e:
                msg = f"Kafka issue in consumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.connect()



kafka_wrapper = KafkaWrapper(
    app_config["events"]["hostname"],
    app_config["events"]["topic"]
)
producer = kafka_wrapper.make_producer()



def report_search_readings(body):
    search_report = body["search_readings"]

    for search in search_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event search_readings with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": search["product_id"],
            "search_count": search["search_count"],
            "recorded_timestamp": search["recorded_timestamp"]
        }

        msg = {
            "type": "search_readings",
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": data 
        }

        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))

        logger.info(f'Produced message to Kafka topic events')
        
    return NoContent, 201


def report_sold_readings(body):
    purchase_report = body["purchase_readings"]

    for purchase in purchase_report:
        trace_id = time.time_ns()
        
        logger.info(f'Received event purchase_reading with trace id {trace_id}')
        
        data = {
            "trace_id": trace_id,
            "store_id": body["store_id"],
            "store_name": body["store_name"],
            "reporting_timestamp": body["reporting_timestamp"],
            "product_id": purchase["product_id"],
            "purchase_count": purchase["purchase_count"],
            "recorded_timestamp": purchase["recorded_timestamp"]
        }

        msg = {
            "type": "purchase_readings",
            "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "payload": data 
        }

        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        
        logger.info(f'Produced message to Kafka topic events')
        
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("/config/grocery_api.yml")

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
