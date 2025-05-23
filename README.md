# Giggityflix Edge Service

Edge service for managing peer connections and routing messages in the Giggityflix media streaming platform.

## Overview

The Edge Service acts as a middleware layer between backend services and peers:

- Implements gRPC server for peer connections
- Routes messages between peers and backend services via Kafka
- Manages and validates peer connections
- Provides discovery mechanism for content availability
- Handles WebRTC session negotiation

## Architecture

### Components

- **gRPC Server**: Manages peer connections with bidirectional streaming
- **Edge Manager**: Maintains mapping between peer_ids and gRPC streams
- **Message Handlers**: Process incoming messages from peers
- **Kafka Adapter**: Converts Kafka messages to gRPC format
- **Kafka Producer/Consumer**: Communicates with backend services

### Connection Flow

1. **Peer Connection**:
    - Peer connects via gRPC with peer_id in metadata
    - Edge validates peer_id, checking:
        - Maximum connection limit
        - Duplicate connections (reject if peer_id already connected)
        - Connection to other edges (via tracker service)
    - If valid, establishes bidirectional stream

2. **Message Routing**:
    - Incoming Kafka messages → Converted to EdgeMessage → Sent to target peer
    - Incoming peer messages → Processed → Published to Kafka

3. **Disconnection**:
    - Detects peer disconnection via stream closure or timeout
    - Removes peer from connection map
    - Publishes disconnection event to Kafka

## Configuration

### Server Configuration

- `EDGE_ID`: Unique ID for this edge instance
- `GRPC_SERVER_ADDRESS`: gRPC server address (default: 0.0.0.0:50051)
- `MAX_PEERS`: Maximum number of concurrent peer connections

### Kafka Configuration

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `PEER_LIFECYCLE_EVENTS_TOPIC`: Topic for peer connection events
- `PEER_CATALOG_UPDATES_TOPIC`: Topic for catalog updates
- `EDGE_COMMANDS_TOPIC_PATTERN`: Pattern for edge command topics

## Development

```bash
# Install dependencies
poetry install

# Run service
poetry run python -m src.main
```

## API

### gRPC Interface

- Handles upto N distinct peer connections across the cluster.
- Requires peer_id in metadata

#### AsyncOperations: Bidirectional streaming for peer-edge communication

- Exchanges PeerMessage and EdgeMessage objects

#### WebRTCOperations: Unary RPC for WebRTC session negotiation

- Exchanges EdgeWebRTCMessage and PeerWebRTCMessage objects

# Giggityflix Edge Functional Requirements

The Edge service functions as the gRPC server that manages connections from multiple Peers, acting as middleware between
backend services and Peers. It provides a validation mechanism that rejects Peer connections when: (1) maximum
connection limit is reached, (2) Peer with same ID attempts to connect twice, or (3) Peer is already connected to
another Edge instance (validated via Tracker service). The Edge maintains a mapping between peer_id and their respective
gRPC streams. For each Kafka message received, the Edge converts it to the appropriate EdgeMessage and routes it to the
target Peer. Similarly, incoming Peer messages are converted and published to Kafka topics. The Edge publishes
connection/disconnection events to Kafka, detects Peer timeouts, and supports both bidirectional streaming (
AsyncOperations) and unary RPC calls (WebRTCOperations). Each Edge operates independently and never communicates with
other Edge instances.