from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

admin = KafkaAdminClient(
    bootstrap_servers='77.81.230.104:9092',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='admin',
    sasl_plain_password='VawEzo1ikLtrA8Ug8THa'
)

# Создаём объект конфигурации для топика
# topic_config = ConfigResource(
#     ConfigResourceType.TOPIC,
#     'rogalev_building_sensors',
#     configs={'retention.ms': '1000'}  # 1 секунда
# )

# # Применяем изменения
# admin.alter_configs([topic_config])
print("Конфигурация топика изменена")

topic_config = ConfigResource(
    ConfigResourceType.TOPIC,
    'rogalev_building_sensors',
    configs={'retention.ms': str(30 * 24 * 60 * 60 * 1000)}  # 30 дней
)

admin.alter_configs([topic_config])
print("Конфигурация топика изменена: retention.ms = 30 дней")

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

admin = KafkaAdminClient(
    bootstrap_servers='77.81.230.104:9092',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='admin',
    sasl_plain_password='VawEzo1ikLtrA8Ug8THa'
)

tp = TopicPartition('rogalev_building_sensors', 0)  # укажи нужный partition
admin.delete_records({tp: 0})  # удаляет все записи до offset 0

tp = TopicPartition('rogalev_building_sensors', 1)  # укажи нужный partition
admin.delete_records({tp: 0})  # удаляет все записи до offset 0