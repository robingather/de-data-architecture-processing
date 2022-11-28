from kafka import KafkaProducer


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def success(metadata):
    print(metadata.topic)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='34.147.0.237:9092')  # use your VM's external IP Here!
    with open('D:/Github Repo/de-data-architecture-processing/data/spotify-stream.txt', encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        kafka_python_producer_sync(producer, line[:-1], 'songs')

    f.close()
