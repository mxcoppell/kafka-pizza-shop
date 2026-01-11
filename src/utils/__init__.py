"""
Utility functions for the pizza shop demo system.

This package contains:
- Kafka producer/consumer creation helpers
- Logging setup utilities
- Common helper functions
"""

import logging
from typing import Dict, Any, Optional
from confluent_kafka import Producer, Consumer


def setup_logging(log_level: str = "INFO", component_name: str = "") -> logging.Logger:
    """
    Set up logging for a component with consistent formatting.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        component_name: Name of the component for log identification
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(component_name or __name__)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Create console handler with formatting
    handler = logging.StreamHandler()
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # Format: [timestamp] [component] [level] message
    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger


def create_kafka_producer(config: Dict[str, Any]) -> Producer:
    """
    Create a Kafka producer with the given configuration.
    
    Args:
        config: Dictionary containing Kafka producer configuration
                Expected keys: bootstrap.servers, and optional producer settings
    
    Returns:
        Configured Kafka Producer instance
    """
    return Producer(config)


def create_kafka_consumer(config: Dict[str, Any]) -> Consumer:
    """
    Create a Kafka consumer with the given configuration.
    
    Args:
        config: Dictionary containing Kafka consumer configuration
                Expected keys: bootstrap.servers, group.id, and optional consumer settings
    
    Returns:
        Configured Kafka Consumer instance
    """
    return Consumer(config)


def delivery_callback(err: Optional[Exception], msg: Any) -> None:
    """
    Callback function for Kafka producer delivery reports.
    
    Args:
        err: Error object if delivery failed, None if successful
        msg: Message object containing metadata about the delivered message
    """
    if err:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(
            f"Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] at offset {msg.offset()}"
        )
