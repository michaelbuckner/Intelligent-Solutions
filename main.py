from flask import Flask, request
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Initialize a Kafka producer pool with compression and batching settings
producer_pool = ThreadPoolExecutor(max_workers=10)
producer_config = {
    'bootstrap_servers': ['localhost:9092'],
    'compression_type': 'gzip',
    'batch_size': 16384,
    'linger_ms': 10,
    'max_batch_size': 1000,
    'value_serializer': lambda x: x.encode('utf-8')
}
kafka_producer = KafkaProducer(**producer_config)

# Create a KafkaAdminClient to create the Kafka topic if it doesn't exist
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
if 'my_topic' not in admin_client.list_topics():
    admin_client.create_topics(new_topics=[('my_topic', 1)])


# Define the endpoint to receive data
@app.route('/send_data', methods=['POST'])
def send_data():
    data = request.json

    # Submit the data to the Kafka producer pool for asynchronous writing
    future = producer_pool.submit(kafka_producer.send, 'my_topic', str(data))
    future.add_done_callback(check_send_result)

    return 'Data sent to Kafka topic: my_topic'


def check_send_result(future):
    try:
        future.result()
    except KafkaError as e:
        print(f'Error sending data to Kafka: {e}')


if __name__ == '__main__':
    app.run(debug=True)
    #foo
