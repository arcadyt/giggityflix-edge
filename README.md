# Giggityflix Edge Service

The Edge Service is a key component of the Giggityflix media streaming platform, responsible for managing peer
connections and routing requests between peers and backend services.

## Overview

The Edge Service:

- Establishes and maintains bidirectional gRPC streams with peers
- Relays messages between peers and backend services via Kafka
- Detects peer disconnections and publishes lifecycle events
- Routes commands from backend services to connected peers

## Architecture

The Edge Service consists of several key components:

- **gRPC Server**: Implements the PeerEdgeService for bidirectional streaming with peers
- **Stream Manager**: Manages active peer connections
- **Message Handler**: Processes various message types
- **Kafka Producer**: Publishes events to Kafka topics
- **Kafka Consumer**: Consumes commands from Kafka topics

## Prerequisites

- Python 3.11 or higher
- Poetry (for dependency management)
- Kafka cluster
- Access to the Giggityflix Proto package

## Installation

```bash
# Clone the repository
git clone https://github.com/giggityflix/edge-service.git
cd edge-service

# Install dependencies with Poetry
poetry install

# Activate the virtual environment
poetry shell
```

## Configuration

The Edge Service can be configured using environment variables:

### Server Configuration

- `EDGE_ID`: Unique ID for this edge instance (default: edge-1)
- `HEARTBEAT_INTERVAL_SEC`: Interval for peer heartbeats (default: 30)
- `PEER_TIMEOUT_SEC`: Timeout for peer connections (default: 120)

### gRPC Configuration

- `GRPC_SERVER_ADDRESS`: gRPC server address (default: 0.0.0.0:50051)
- `GRPC_USE_TLS`: Enable TLS for gRPC (default: true)
- `GRPC_CERT_PATH`: Path to TLS certificate (default: certs/server.crt)
- `GRPC_KEY_PATH`: Path to TLS key (default: certs/server.key)
- `GRPC_MAX_WORKERS`: Maximum number of gRPC workers (default: 10)

### Kafka Configuration

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses (default: localhost:9092)
- `KAFKA_GROUP_ID`: Consumer group ID (default: edge-service)
- `KAFKA_EDGE_COMMANDS_TOPIC_PATTERN`: Pattern for edge commands topic (default: edge-commands-{})
- `KAFKA_PEER_LIFECYCLE_EVENTS_TOPIC`: Topic for peer lifecycle events (default: peer-lifecycle-events)
- `KAFKA_PEER_CATALOG_UPDATES_TOPIC`: Topic for peer catalog updates (default: peer-catalog-updates)
- `KAFKA_FILE_DELETE_RESPONSES_TOPIC`: Topic for file delete responses (default: file-delete-responses)
- `KAFKA_FILE_HASH_RESPONSES_TOPIC`: Topic for file hash responses (default: file-hash-responses)
- `KAFKA_SCREENSHOT_CAPTURE_RESPONSES_TOPIC`: Topic for screenshot capture responses (default:
  screenshot-capture-responses)
- `KAFKA_DEADLETTER_TOPIC_PATTERN`: Pattern for deadletter queue topic (default: edge-commands-{}.deadletter)

### Retry Configuration

- `RETRY_MAX_RETRIES`: Maximum number of retries (default: 3)
- `RETRY_INITIAL_DELAY_MS`: Initial delay for retries (default: 100)
- `RETRY_MAX_DELAY_MS`: Maximum delay for retries (default: 1000)
- `RETRY_JITTER_FACTOR`: Jitter factor for retries (default: 0.1)

## Running the Service

```bash
# Start the service
poetry run python -m src.main
```

## Message Flow

1. **Peer Registration**:
    - Peer sends a PeerRegistrationRequest to the Edge Service
    - Edge Service registers the peer and publishes a peer-connected event
    - Edge Service responds with a PeerRegistrationResponse

2. **Command Routing**:
    - Backend services publish commands to the edge-commands-{edge_id} topic
    - Edge Service consumes commands and routes them to the appropriate peer
    - Peer responds to the command
    - Edge Service publishes the response to the appropriate topic

3. **Peer Disconnection**:
    - Edge Service detects peer disconnection (gRPC stream closure)
    - Edge Service publishes a peer-disconnected event

## Development

### Running Tests

```bash
# Run unit tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src
```

## Monitoring

The Edge Service logs all significant events to aid in monitoring and debugging. Logs include:

- Peer connections and disconnections
- Message routing
- Kafka events
- Errors and retries

## Error Handling

The Edge Service implements robust error handling:

- Exponential backoff with jitter for retries
- Dead-letter queue for undeliverable messages
- Graceful handling of peer disconnections
- Comprehensive logging
