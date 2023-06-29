from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask
import threading
import yaml

app = Flask(__name__)
stop = threading.Event()

KAFKA_BOOTSTRAP_SERVERS = ''
KAFKA_SSL_CA_LOCATION = ''
KAFKA_SSL_CERTIFICATE_LOCATION = ''
KAFKA_TOPIC = 'website-status'

status_dict = {}


def kafka_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'SSL',
        'ssl.ca.location': KAFKA_SSL_CA_LOCATION,
        'ssl.certificate.location': KAFKA_SSL_CERTIFICATE_LOCATION,
        'group.id': 'website-status-group',
        'auto.offset.reset': 'earliest',
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error occurred: {}'.format(msg.error().str()))
                    continue

            # Process the received Kafka message here
            process_kafka_message(msg)

    except KeyboardInterrupt:
        consumer.close()


def process_kafka_message(message):
    # Do any processing or formatting of the Kafka message here
    try:
        message_key_utf8 = message.key().decode('utf-8')
        message_status_utf8 = message.value().decode('utf-8')
        print('Status for:', message_key_utf8, "is", message_status_utf8)
        # The status dict should have the url of each website as the key and point to a list of statuses
        # If there is no dict entry for the url, create one
        if message_key_utf8 not in status_dict:
            status_dict[message_key_utf8] = []

        status_dict[message_key_utf8].append(message_status_utf8)
    except UnicodeDecodeError:
        print('Received Kafka message:', message)


@app.route('/')
def serve_kafka_data():
    return status_dict


def get_config_from_yaml():
    with open("../src/main/resources/kafka_config.yaml", "r") as stream:
        # Intentionally don't handle error here, if the file is missing, the app should crash
        yaml_contents = yaml.safe_load(stream)

        global KAFKA_BOOTSTRAP_SERVERS
        global KAFKA_SSL_CA_LOCATION
        global KAFKA_SSL_CERTIFICATE_LOCATION

        KAFKA_BOOTSTRAP_SERVERS = yaml_contents['bootstrapServers']
        KAFKA_SSL_CA_LOCATION = yaml_contents['sslCaLocation']
        KAFKA_SSL_CERTIFICATE_LOCATION = yaml_contents['sslCertificateLocation']


if __name__ == '__main__':
    get_config_from_yaml()
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.start()

    # Start the Flask web server
    app.run()
