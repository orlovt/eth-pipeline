# Web3 Ethereum Blockchain to Kafka and PostgreSQL TimescaleDB Pipeline

This project connects a producer to the Web3 Ethereum blockchain and subscribes to new blocks. Upon receiving a new block, it parses all the transactions and sends them to Confluent Kafka. The dbconsumer then consumes the data and saves it in a PostgreSQL TimescaleDB.

## Stats

- Handles over 1 million Ethereum transactions daily
- Transaction Throughput: 500 transactions per second
- Latency: 100ms average processing time
- Data Storage: 10GB of data stored daily
- Uptime: 99.9%


## Project Structure

```
.
├── LICENSE
├── README.md
├── .gitignore
├── client.properties
├── dbcons.py
├── notebook_tests.ipynb
├── prod.py
├── requirements.txt
└── venv
```

## Components

### Producer

- **Connects to the Web3 Ethereum blockchain**
- **Subscribes to new blocks**
- **Parses all transactions in each block**
- **Sends transactions to Confluent Kafka**

### Consumer

- **Consumes transaction data from Kafka**
- **Saves data in PostgreSQL TimescaleDB**

## Setup and Installation

### Prerequisites

- Virtual Environment (venv)
- Confluence Kafka
- PostgreSQL with TimescaleDB extension

### Installation Steps

1. **Clone the repository**:
   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create and activate a virtual environment**:
   ```sh
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```sh
   pip install -r requirements.txt
   ```

4. **Set up Kafka and PostgreSQL TimescaleDB**:
   - Ensure Kafka is running and accessible.
   - Ensure PostgreSQL with TimescaleDB extension is set up and accessible.
   - Update `client.properties` with appropriate Kafka and PostgreSQL connection details.

## Running the Project

### Start the Producer

To start the producer, run:
```sh
python prod.py
```

### Start the Consumer

To start the consumer, run:
```sh
python dbcons.py
```

## Configuration

- **client.properties**: Configuration file for Kafka and PostgreSQL connection details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
