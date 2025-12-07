from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random


# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
#my_name = "oleksiy"
topic_name = 'rogalev_building_sensors'

sensorID=str(uuid.uuid4())
print(f"Sensor ID: {sensorID} topic_name: {topic_name}")

for i in range(300):
    # Відправлення повідомлення в топік
    try:
        data = {
            "sensorID": sensorID,
            "sensorType": "temperature",
            "timestamp": time.time(),  # Часова мітка
            "value": random.randint(25, 45)  # Випадкове значення
        }
        key=str(data["timestamp"])
        print(f"key: {key}")
        producer.send(topic_name, key=key, value=data) # key=str(data["timestamp"]).encode('utf-8'),
        #producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully. data: {data}")
        time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped by user.")
        break 
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer

