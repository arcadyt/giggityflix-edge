import os
from typing import Optional

from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    group_id: str = Field(default_factory=lambda: os.environ.get("KAFKA_GROUP_ID", "edge-service"))
    
    # Consumed topics
    edge_commands_topic_pattern: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_EDGE_COMMANDS_TOPIC_PATTERN", "edge-commands-{}"))
    
    # Produced topics
    peer_lifecycle_events_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_PEER_LIFECYCLE_EVENTS_TOPIC", "peer-lifecycle-events"))
    peer_catalog_updates_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_PEER_CATALOG_UPDATES_TOPIC", "peer-catalog-updates"))
    
    # Service-specific response topics
    file_delete_responses_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_FILE_DELETE_RESPONSES_TOPIC", "file-delete-responses"))
    file_hash_responses_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_FILE_HASH_RESPONSES_TOPIC", "file-hash-responses"))
    screenshot_capture_responses_topic: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_SCREENSHOT_CAPTURE_RESPONSES_TOPIC", "screenshot-capture-responses"))
    
    # Dead letter queue
    deadletter_topic_pattern: str = Field(
        default_factory=lambda: os.environ.get("KAFKA_DEADLETTER_TOPIC_PATTERN", "edge-commands-{}.deadletter"))


class GrpcConfig(BaseModel):
    server_address: str = Field(default_factory=lambda: os.environ.get("GRPC_SERVER_ADDRESS", "0.0.0.0:50051"))
    use_tls: bool = Field(default_factory=lambda: os.environ.get("GRPC_USE_TLS", "true").lower() == "true")
    cert_path: str = Field(default_factory=lambda: os.environ.get("GRPC_CERT_PATH", "certs/server.crt"))
    key_path: str = Field(default_factory=lambda: os.environ.get("GRPC_KEY_PATH", "certs/server.key"))
    max_workers: int = Field(default_factory=lambda: int(os.environ.get("GRPC_MAX_WORKERS", "10")))


class RetryConfig(BaseModel):
    max_retries: int = Field(default_factory=lambda: int(os.environ.get("RETRY_MAX_RETRIES", "3")))
    initial_delay_ms: int = Field(default_factory=lambda: int(os.environ.get("RETRY_INITIAL_DELAY_MS", "100")))
    max_delay_ms: int = Field(default_factory=lambda: int(os.environ.get("RETRY_MAX_DELAY_MS", "1000")))
    jitter_factor: float = Field(default_factory=lambda: float(os.environ.get("RETRY_JITTER_FACTOR", "0.1")))


class EdgeConfig(BaseModel):
    edge_id: str = Field(default_factory=lambda: os.environ.get("EDGE_ID", "edge-1"))
    heartbeat_interval_sec: int = Field(default_factory=lambda: int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "30")))
    peer_timeout_sec: int = Field(default_factory=lambda: int(os.environ.get("PEER_TIMEOUT_SEC", "120")))


class Config(BaseModel):
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    grpc: GrpcConfig = Field(default_factory=GrpcConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    edge: EdgeConfig = Field(default_factory=EdgeConfig)


# Singleton config instance
config = Config()
