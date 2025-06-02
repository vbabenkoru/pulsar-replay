# Pulsar Backup and Restore Tool

This Python script allows you to create a full snapshot of a Pulsar cluster, save it to files, and restore it. It preserves message metadata, properties, and other attributes.

## Features

- Capture Pulsar tenants, namespaces, topics, and messages with metadata
- Restore Pulsar tenants, namespaces, and topics
- Replay captured messages with original properties and metadata
- Supports message properties and metadata that aren't easily added via the command line

## Requirements

- Python 3.6+
- Apache Pulsar client for Python
- pulsarctl CLI tool

## Installation

1. Install the required Python dependencies:

```
pip install -r requirements.txt
```

2. Make sure you have pulsarctl installed and accessible in your PATH.

## Configuration

Edit the configuration variables at the top of `pulsar_backup_restore.py`:

```python
PULSAR_URL = "pulsar://localhost:6650"
CAPTURE_DIR = "pulsar_backup"
DOCKER_CONTAINER = "iterable-arm64-pulsar_standalone-1"
```

## Usage

Run the script:

```
python pulsar_backup_restore.py
```

Choose from the following options:

1. Capture Pulsar tenants, namespaces, topics, and messages
2. Restore Pulsar tenants, namespaces, and topics
3. Replay captured messages

## Data Storage

All backup data is stored in the `pulsar_backup` directory:
- `tenants.txt`: List of tenants
- `namespaces.txt`: List of namespaces
- `topics.txt`: List of topics
- `messages/`: Directory containing JSON files with messages and their metadata

## Example Message JSON Structure

Each message is stored with its content and all relevant metadata:

```json
{
  "content": "message content",
  "properties": {
    "property1": "value1",
    "property2": "value2"
  },
  "publish_timestamp": 1647456789000,
  "event_timestamp": 1647456789000,
  "partition_key": "key1"
}
``` 