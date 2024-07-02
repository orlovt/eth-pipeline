from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

def read_config():
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        print("Configuration loaded:", config)
    except Exception as e:
        print(f"Failed to read config file: {e}")
    return config

def consume_messages(config, topic):
    consumer_config = config.copy()
    consumer_config.update({
        'group.id': 'python-group-1',
        'auto.offset.reset': 'earliest'
    })

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    print("Consumer subscribed to topic:", topic)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print('Consumed message from topic {}: key = {}'.format(
                    msg.topic(), msg.key().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    config = read_config()
    topic = "eth-transaction-client"
    consume_messages(config, topic)

if __name__ == "__main__":
    main()
