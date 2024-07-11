import json
import asyncio
import asyncpg
from confluent_kafka import Consumer, KafkaException, KafkaError
from datetime import datetime
import sys
import os

CONNECTION = os.getenv('POSTGRES_CONNECTION')


async def create_timescale_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS transactions (
        block_number INTEGER,
        timestamp TIMESTAMPTZ,
        transaction_hash TEXT PRIMARY KEY,
        sender TEXT,
        receiver TEXT,
        value DOUBLE PRECISION,
        gas BIGINT,
        gas_price DOUBLE PRECISION,
        nonce BIGINT,
        transaction_index INTEGER,
        gas_used BIGINT,
        cumulative_gas_used BIGINT,
        gas_limit BIGINT,
        receipt_status INTEGER,
        transaction_fee DOUBLE PRECISION,
        effective_gas_price DOUBLE PRECISION
    );
    """
    await conn.execute(create_table_query)

async def insert_transaction(conn, transaction):
    # Convert timestamp string to datetime object
    transaction['timestamp'] = datetime.strptime(transaction['timestamp'], '%Y-%m-%d %H:%M:%S')

    insert_query = """
    INSERT INTO transactions (
        block_number, timestamp, transaction_hash, sender, receiver, value, gas, gas_price, nonce,
        transaction_index, gas_used, cumulative_gas_used, gas_limit, receipt_status, transaction_fee, effective_gas_price
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
    ) ON CONFLICT (transaction_hash) DO NOTHING;
    """
    await conn.execute(insert_query, transaction['block_number'], transaction['timestamp'], transaction['transaction_hash'], 
                       transaction['from'], transaction['to'], transaction['value'], transaction['gas'], 
                       transaction['gas_price'], transaction['nonce'], transaction['transaction_index'], 
                       transaction['gas_used'], transaction['cumulative_gas_used'], transaction['gas_limit'], 
                       transaction['receipt_status'], transaction['transaction_fee'], transaction['effective_gas_price'])

async def consume_messages(config, topic):
    conn = await asyncpg.connect(CONNECTION)
    await create_timescale_table(conn)

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
                transaction_data = json.loads(msg.value().decode('utf-8'))
                await insert_transaction(conn, transaction_data)
                print(f"Inserted transaction {transaction_data['transaction_hash']}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        await conn.close()

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

def main():
    config = read_config()
    topic = "eth-transaction-client"
    asyncio.run(consume_messages(config, topic))

if __name__ == "__main__":
    main()
