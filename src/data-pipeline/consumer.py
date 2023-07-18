from confluent_kafka import Consumer, KafkaError
import json
import ccloud_lib
import time
import pandas as pd

# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "velib-metropole"

# Create Consumer instance
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'velib_consumer'
consumer_conf['auto.offset.reset'] = 'earliest'
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC])

# Process messages
try:
    while True:
        msg = consumer.poll(timeout=-1)
        if msg is None:
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('Error: {}'.format(msg.error()))
        else:
            record_key = msg.key().decode("utf-8")
            record_value = msg.value().decode("utf-8")
            
            if record_key == "velib_station":
                data = json.loads(record_value)
                print("Données de velib-disponibilite-en-temps-reel:")
                print(data)
            elif record_key == "velib_status":
                data = json.loads(record_value)
                print("Données de velib-emplacement-des-stations:")
                print(data)
        
        time.sleep(2.0)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
