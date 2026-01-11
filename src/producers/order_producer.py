"""
Order producer for the pizza shop demo system.

Handles creation and publishing of pizza orders to Kafka topics.
"""

import json
import time
import random
from typing import Optional
from confluent_kafka import Producer, KafkaException

from src.config import (
    get_producer_config,
    TOPIC_PIZZA_ORDERS,
    TOPIC_ORDER_EVENTS,
)
from src.models import (
    create_order_message,
    create_order_event_message,
    generate_customer_name,
    EventType,
    OrderMessage,
)
from src.utils import setup_logging, create_kafka_producer, delivery_callback


class OrderProducer:
    """
    Producer for pizza orders.
    
    Handles order creation and publishing to Kafka topics:
    - Publishes orders to pizza-orders topic
    - Publishes ORDER_CREATED events to order-events topic
    """
    
    def __init__(self, source: str, logger_name: Optional[str] = None):
        """
        Initialize the order producer.
        
        Args:
            source: Order source type ("web" or "frontdesk")
            logger_name: Optional logger name (defaults to source-based name)
        """
        self.source = source
        self.logger_name = logger_name or f"{source}_producer"
        self.logger = setup_logging(component_name=self.logger_name)
        
        # Initialize Kafka producer
        self.producer: Optional[Producer] = None
        self.order_counter = 0
        
        # Statistics
        self.orders_published = 0
        self.events_published = 0
        self.errors = 0
        
    def connect(self) -> None:
        """
        Connect to Kafka and create producer instance.
        
        Raises:
            KafkaException: If connection fails
        """
        try:
            config = get_producer_config()
            config["client.id"] = f"pizza-shop-{self.source}-producer"
            self.producer = create_kafka_producer(config)
            self.logger.info(f"Connected to Kafka as {self.source} producer")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise KafkaException(f"Kafka connection failed: {e}")
    
    def _publish_message(self, topic: str, key: str, message: dict) -> bool:
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            key: Message key (used for partitioning)
            message: Message dictionary to publish
            
        Returns:
            True if published successfully, False otherwise
        """
        if not self.producer:
            self.logger.error("Producer not connected. Call connect() first.")
            return False
        
        try:
            # Serialize message to JSON
            value = json.dumps(message).encode('utf-8')
            key_bytes = key.encode('utf-8')
            
            # Publish to Kafka
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                callback=delivery_callback
            )
            
            # Poll to handle delivery callbacks
            self.producer.poll(0)
            
            return True
            
        except BufferError:
            # Queue is full, wait and retry
            self.logger.warning(f"Local queue full for {topic}, waiting...")
            self.producer.poll(1)
            return self._publish_message(topic, key, message)
            
        except Exception as e:
            self.logger.error(f"Failed to publish to {topic}: {e}")
            self.errors += 1
            return False
    
    def publish_order(self, order_number: int, customer_name: Optional[str] = None) -> Optional[OrderMessage]:
        """
        Create and publish a pizza order.
        
        Args:
            order_number: Sequential order number
            customer_name: Optional customer name (generated if not provided)
            
        Returns:
            OrderMessage if successful, None otherwise
        """
        try:
            # Generate customer name if not provided
            if customer_name is None:
                customer_name = generate_customer_name()
            
            # Create order message
            order = create_order_message(
                source=self.source,
                customer_name=customer_name,
                order_number=order_number
            )
            
            # Publish order to pizza-orders topic
            success = self._publish_message(
                topic=TOPIC_PIZZA_ORDERS,
                key=order["order_id"],
                message=order
            )
            
            if not success:
                self.logger.error(f"Failed to publish order #{order_number}")
                return None
            
            self.orders_published += 1
            self.logger.info(
                f"Published order #{order_number}: {customer_name} - "
                f"{order['total_pizzas']} pizza(s) from {self.source}"
            )
            
            # Publish ORDER_CREATED event to order-events topic
            event = create_order_event_message(
                order_id=order["order_id"],
                event_type=EventType.ORDER_CREATED.value,
                actor=f"{self.source}_interface",
                order_number=order["order_number"],
                customer_name=order["customer_name"],
                total_pizzas=order["total_pizzas"],
                metadata={"source": self.source}
            )
            
            success = self._publish_message(
                topic=TOPIC_ORDER_EVENTS,
                key=order["order_id"],
                message=event
            )
            
            if success:
                self.events_published += 1
                self.logger.debug(f"Published ORDER_CREATED event for order #{order_number}")
            else:
                self.logger.warning(f"Failed to publish event for order #{order_number}")
            
            return order
            
        except Exception as e:
            self.logger.error(f"Error publishing order #{order_number}: {e}")
            self.errors += 1
            return None
    
    def flush(self, timeout: float = 10.0) -> None:
        """
        Flush any pending messages.
        
        Args:
            timeout: Maximum time to wait for flushing (seconds)
        """
        if self.producer:
            try:
                self.logger.info("Flushing pending messages...")
                remaining = self.producer.flush(timeout)
                if remaining > 0:
                    self.logger.warning(f"{remaining} messages were not flushed")
                else:
                    self.logger.info("All messages flushed successfully")
            except Exception as e:
                self.logger.error(f"Error flushing producer: {e}")
    
    def close(self) -> None:
        """Close the producer and cleanup resources."""
        if self.producer:
            try:
                self.flush()
            except Exception as e:
                self.logger.error(f"Error during producer flush on close: {e}")
            finally:
                self.producer = None
                self.logger.info(f"{self.source} producer closed")
    
    def get_statistics(self) -> dict:
        """
        Get producer statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            "source": self.source,
            "orders_published": self.orders_published,
            "events_published": self.events_published,
            "errors": self.errors,
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup even on exceptions."""
        try:
            if exc_type is KeyboardInterrupt:
                self.logger.info("Received KeyboardInterrupt - closing producer gracefully")
            elif exc_type is not None:
                self.logger.error(f"Exception during producer operation: {exc_val}")
        finally:
            self.close()
        # Don't suppress the exception
        return False
