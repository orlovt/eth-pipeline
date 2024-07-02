import asyncio
import json
import websockets
from datetime import datetime
from web3 import Web3
from confluent_kafka import Producer, Consumer

# Define Infura project ID
infura_project_id = 'a30f6d61930e4435aecdd1b6815f2026'

# # Connect to the Infura HTTP endpoint
web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_project_id}'))

def read_config():
    # Reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce(topic, config, block_data):
    # Creates a new producer instance
    producer = Producer(config)
    # Produces a sample message
    producer.produce(topic, key=str(block_data['block_number']), value=json.dumps(block_data), callback=delivery_report)
    producer.flush()

async def subscribe_new_blocks(config):
    infura_project_id = 'a30f6d61930e4435aecdd1b6815f2026'
    infura_url = f'wss://mainnet.infura.io/ws/v3/{infura_project_id}'

    async with websockets.connect(infura_url) as websocket:
        subscription_request = json.dumps({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        })

        await websocket.send(subscription_request)
        print("Subscribed to new block headers")

        while True:
            try:
                response = await websocket.recv()
                response_json = json.loads(response)

                if 'params' in response_json:
                    block_header = response_json['params']['result']
                    block_number = block_header['number']
                    block = web3.eth.get_block(block_number, full_transactions=True)
                    print(f"New block received: {block['number']}")
                    analyze_block(block, config)

            except websockets.ConnectionClosed:
                print("Connection closed, attempting to reconnect")
                await asyncio.sleep(1)
                continue

def analyze_block(block, config):
    transaction_count = len(block.transactions)
    total_value = sum(tx['value'] for tx in block.transactions)
    average_value = total_value / transaction_count if transaction_count > 0 else 0
    timestamp = datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

    block_data = {
        'block_number': block['number'],
        'timestamp': timestamp,
        'transaction_count': transaction_count,
        'total_value': float(web3.from_wei(total_value, 'ether')),
        'average_value': float(web3.from_wei(average_value, 'ether'))
    }

    print(f"Block Number: {block['number']}")
    print(f"Timestamp: {timestamp} UTC")
    print(f"Transaction Count: {transaction_count}")
    print(f"Total Transaction Value: {block_data['total_value']} ETH")
    print(f"Average Transaction Value: {block_data['average_value']} ETH")

    # Send block data to Kafka
    produce('eth-transaction-client', config, block_data)

async def main():
    config = read_config()
    global web3
    infura_project_id = 'a30f6d61930e4435aecdd1b6815f2026'
    web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_project_id}'))

    await subscribe_new_blocks(config)

if __name__ == "__main__":
    asyncio.run(main())
