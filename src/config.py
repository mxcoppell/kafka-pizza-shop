"""
Configuration module for the pizza shop demo system.

Loads configuration from environment variables and provides
constants for Kafka connections, topics, and demo parameters.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()


# ============================================================================
# Kafka Connection Configuration
# ============================================================================

def get_kafka_bootstrap_servers() -> str:
    """
    Get Kafka bootstrap servers based on environment.
    
    Returns HOST version for local development, DOCKER version for containers.
    """
    # Check if running in Docker (common env var set by Docker)
    if os.getenv("DOCKER_CONTAINER"):
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    else:
        return os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9093")


KAFKA_BOOTSTRAP_SERVERS = get_kafka_bootstrap_servers()


# ============================================================================
# Kafka Topic Names
# ============================================================================

TOPIC_PIZZA_ORDERS = os.getenv("TOPIC_PIZZA_ORDERS", "pizza-orders")
TOPIC_COOKED_PIZZAS = os.getenv("TOPIC_COOKED_PIZZAS", "cooked-pizzas")
TOPIC_ORDER_EVENTS = os.getenv("TOPIC_ORDER_EVENTS", "order-events")


# ============================================================================
# Consumer Group IDs
# ============================================================================

CONSUMER_GROUP_PIZZA_MAKERS = os.getenv("CONSUMER_GROUP_PIZZA_MAKERS", "pizza-makers")
CONSUMER_GROUP_DELIVERY_WORKERS = os.getenv("CONSUMER_GROUP_DELIVERY_WORKERS", "delivery-workers")
CONSUMER_GROUP_BOOKKEEPER = os.getenv("CONSUMER_GROUP_BOOKKEEPER", "bookkeeper")


# ============================================================================
# Demo Configuration Parameters
# ============================================================================

# Total number of orders to generate
TOTAL_ORDERS = int(os.getenv("TOTAL_ORDERS", "200"))

# Order distribution between sources
WEB_ORDERS = int(os.getenv("WEB_ORDERS", "120"))
FRONTDESK_ORDERS = int(os.getenv("FRONTDESK_ORDERS", "80"))


# ============================================================================
# Component Identifiers
# ============================================================================

PIZZA_MAKER_1_ID = os.getenv("PIZZA_MAKER_1_ID", "pizza_maker_1")
PIZZA_MAKER_2_ID = os.getenv("PIZZA_MAKER_2_ID", "pizza_maker_2")
PIZZA_MAKER_3_ID = os.getenv("PIZZA_MAKER_3_ID", "pizza_maker_3")

DELIVERY_DRIVER_1_ID = os.getenv("DELIVERY_DRIVER_1_ID", "delivery_driver_1")
DELIVERY_DRIVER_2_ID = os.getenv("DELIVERY_DRIVER_2_ID", "delivery_driver_2")
FRONTDESK_DELIVERY_ID = os.getenv("FRONTDESK_DELIVERY_ID", "frontdesk_delivery")


# ============================================================================
# Demo Timing Configuration
# ============================================================================

# Multiplier for cooking times (1.0 = normal speed, 0.5 = half speed, etc.)
COOKING_TIME_MULTIPLIER = float(os.getenv("COOKING_TIME_MULTIPLIER", "1.0"))

# Delivery time range in seconds
DELIVERY_TIME_MIN = float(os.getenv("DELIVERY_TIME_MIN", "1.0"))
DELIVERY_TIME_MAX = float(os.getenv("DELIVERY_TIME_MAX", "3.0"))


# ============================================================================
# Logging Configuration
# ============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# ============================================================================
# Kafka Producer Configuration
# ============================================================================

def get_producer_config() -> Dict[str, Any]:
    """
    Get Kafka producer configuration dictionary.
    
    Returns:
        Dictionary with Kafka producer settings
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": os.getenv("PRODUCER_ACKS", "1"),
        "compression.type": os.getenv("PRODUCER_COMPRESSION_TYPE", "snappy"),
        "message.max.bytes": int(os.getenv("PRODUCER_MAX_REQUEST_SIZE", "1048576")),
        "client.id": "pizza-shop-producer",
    }


# ============================================================================
# Kafka Consumer Configuration
# ============================================================================

def get_consumer_config(group_id: str, client_id: str = "pizza-shop-consumer") -> Dict[str, Any]:
    """
    Get Kafka consumer configuration dictionary.
    
    Args:
        group_id: Consumer group ID
        client_id: Client identifier (optional)
        
    Returns:
        Dictionary with Kafka consumer settings
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "client.id": client_id,
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": os.getenv("ENABLE_AUTO_COMMIT", "true").lower() == "true",
        "auto.commit.interval.ms": int(os.getenv("AUTO_COMMIT_INTERVAL_MS", "1000")),
        "session.timeout.ms": int(os.getenv("SESSION_TIMEOUT_MS", "30000")),
        "max.poll.interval.ms": int(os.getenv("MAX_POLL_INTERVAL_MS", "300000")),
    }


# ============================================================================
# Order Generation Timing Patterns
# ============================================================================

# Order timing patterns based on architecture document
ORDER_TIMING_PATTERNS = {
    "initial_burst": {
        "range": (1, 50),
        "delay_min": 0.05,
        "delay_max": 0.1,
    },
    "steady_flow": {
        "range": (51, 150),
        "delay_min": 0.1,
        "delay_max": 0.3,
    },
    "final_rush": {
        "range": (151, 200),
        "delay_min": 0.05,
        "delay_max": 0.1,
    },
}


# ============================================================================
# Pizza Distribution Weights
# ============================================================================

# Distribution weights for pizza selection
# Simple (50%), Medium (35%), Complex (15%)
PIZZA_DISTRIBUTION_WEIGHTS = {
    "simple": 0.50,
    "medium": 0.35,
    "complex": 0.15,
}

# Pizzas per order distribution
PIZZAS_PER_ORDER_DISTRIBUTION = {
    1: 0.50,  # 50% of orders have 1 pizza
    2: 0.35,  # 35% of orders have 2 pizzas
    3: 0.15,  # 15% of orders have 3 pizzas
}
