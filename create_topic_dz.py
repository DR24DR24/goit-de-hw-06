from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
#my_name = "oleksiy"
#topic_name = f'{my_name}_topic_1'
num_partitions = 2
replication_factor = 1

building_sensors_topic = NewTopic(name="rogalev_building_sensors", 
                                  num_partitions=num_partitions, replication_factor=replication_factor)
temperature_alerts_topic = NewTopic(name="rogalev_temperature_alerts", 
                                    num_partitions=num_partitions, replication_factor=replication_factor)
humidity_alerts_topic = NewTopic(name="rogalev_humidity_alerts", 
                                 num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[building_sensors_topic,
                                           temperature_alerts_topic,humidity_alerts_topic], validate_only=False)
    print(f"Topics  created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків 
print(admin_client.list_topics())

# Закриття зв'язку з клієнтом
admin_client.close()

