import asyncio
import json
import websockets
from datetime import datetime
from web3 import Web3
from confluent_kafka import Producer


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

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_batch(producer, topic, transactions):
    try:
        for transaction_data in transactions:
            print(f"Producing transaction: {transaction_data['transaction_hash']}")
            producer.produce(topic, key=transaction_data['transaction_hash'], value=json.dumps(transaction_data), callback=delivery_report)
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message: {e}")

async def subscribe_new_blocks(config, topic):
    infura_project_id = 'a30f6d61930e4435aecdd1b6815f2026'
    infura_url = f'wss://mainnet.infura.io/ws/v3/{infura_project_id}'

    try:
        print("Connecting to Infura WebSocket...")
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
                        block_number = int(block_header['number'], 16)
                        print(f"New block header received: {block_number}")
                        block = web3.eth.get_block(block_number, full_transactions=True)
                        print(f"Full block data retrieved for block number: {block['number']}")
                        analyze_block(block, config, topic)

                except websockets.ConnectionClosed:
                    print("Connection closed, attempting to reconnect")
                    await asyncio.sleep(1)
                    continue
    except Exception as e:
        print(f"Error in WebSocket connection: {e}")


def analyze_block(block, config, topic):
    """
    Analyzes a block and produces the transactions in batch to a Kafka topic.

    Args:
        block (dict): The block to analyze.
        config (dict): The configuration settings.
        topic (str): The Kafka topic to produce the transactions to.
    """
    timestamp = datetime.fromtimestamp(block['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Analyzing block {block['number']} with {len(block.transactions)} transactions")

    transactions = []
    print(f"Producing top 50 transactions for block {block['number']}")
    for tx in block.transactions[:50]:
        receipt = web3.eth.get_transaction_receipt(tx['hash'])
        transaction_data = {
            'block_number': block['number'],
            'timestamp': timestamp,
            'transaction_hash': tx['hash'].hex(),
            'from': tx['from'],
            'to': tx['to'],
            'value': float(Web3.from_wei(tx['value'], 'ether')),
            'gas': tx['gas'],
            'gas_price': float(Web3.from_wei(tx['gasPrice'], 'gwei')),
            'nonce': tx['nonce'],
            'transaction_index': tx['transactionIndex'],
            'gas_used': receipt['gasUsed'],
            'cumulative_gas_used': receipt['cumulativeGasUsed'],
            'gas_limit': tx['gas'],
            'receipt_status': receipt['status'],
            'transaction_fee': float(Web3.from_wei(receipt['gasUsed'] * tx['gasPrice'], 'ether')),
            'effective_gas_price': float(Web3.from_wei(receipt['effectiveGasPrice'], 'gwei'))
        }
        transactions.append(transaction_data)
    
    # Produce transactions in batch
    producer = Producer(config)
    produce_batch(producer, topic, transactions)


async def main():
    config = read_config()
    topic = "eth-transaction-client"

    # Define Web3 globally
    global web3
    infura_project_id = 'a30f6d61930e4435aecdd1b6815f2026'
    web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_project_id}'))

    await subscribe_new_blocks(config, topic)

if __name__ == "__main__":
    asyncio.run(main())
