"""
Delivery Worker consumer for the pizza shop demo system.

Consumes cooked pizzas from the cooked-pizzas topic, simulates delivery time,
and publishes delivery events to the order-events topic.
"""

import sys
import json
import time
import random
import signal
from typing import Optional, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from src.config import (
    get_consumer_config,
    get_producer_config,
    TOPIC_COOKED_PIZZAS,
    TOPIC_ORDER_EVENTS,
    CONSUMER_GROUP_DELIVERY_WORKERS,
    DELIVERY_TIME_MIN,
    DELIVERY_TIME_MAX,
    LOG_LEVEL,
)
from src.models import (
    CookedPizzaMessage,
    create_order_event_message,
    EventType,
)
from src.utils import setup_logging, create_kafka_consumer, create_kafka_producer


class DeliveryWorker:
    """
    Delivery Worker consumer that delivers cooked pizzas to customers.
    
    Consumes from cooked-pizzas topic, simulates delivery time,
    and publishes delivery events to order-events topic.
    """
    
    def __init__(self, worker_id: str, worker_type: str):
        """
        Initialize the delivery worker.
        
        Args:
            worker_id: Unique identifier for this delivery worker (e.g., "driver-1")
            worker_type: Type of worker (e.g., "frontdesk", "driver")
        """
        self.worker_id = worker_id
        self.worker_type = worker_type
        self.logger = setup_logging(LOG_LEVEL, f"DeliveryWorker-{worker_id}")
        self.running = False
        
        # Create consumer for cooked-pizzas topic
        consumer_config = get_consumer_config(
            group_id=CONSUMER_GROUP_DELIVERY_WORKERS,
            client_id=f"delivery-worker-{worker_id}"
        )
        self.consumer = create_kafka_consumer(consumer_config)
        
        # Create producer for order-events topic
        producer_config = get_producer_config()
        producer_config["client.id"] = f"delivery-worker-{worker_id}"
        self.producer = create_kafka_producer(producer_config)
        
        # Subscribe to cooked-pizzas topic
        self.consumer.subscribe([TOPIC_COOKED_PIZZAS])
        
        self.logger.info(f"Delivery Worker '{worker_id}' ({worker_type}) initialized")
        self.logger.info(f"Subscribed to topic: {TOPIC_COOKED_PIZZAS}")
        self.logger.info(f"Consumer group: {CONSUMER_GROUP_DELIVERY_WORKERS}")
    
    def _delivery_callback(self, err: Optional[Exception], msg: Any) -> None:
        """Callback for producer delivery reports."""
        if err:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
    
    def _publish_message(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Target topic name
            key: Message key (for partitioning)
            value: Message value dictionary
        """
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)  # Trigger callbacks
        except Exception as e:
            self.logger.error(f"Failed to publish to {topic}: {e}")
    
    def _deliver_order(self, cooked_pizza: CookedPizzaMessage) -> None:
        """
        Deliver a cooked pizza order to the customer.
        
        Args:
            cooked_pizza: Cooked pizza message to deliver
        """
        order_id = cooked_pizza["order_id"]
        order_number = cooked_pizza["order_number"]
        customer_name = cooked_pizza["customer_name"]
        total_pizzas = cooked_pizza["total_pizzas"]
        cooked_by = cooked_pizza["cooked_by"]
        
        self.logger.info(
            f"Started delivery for order #{order_number} (ID: {order_id[:8]}...) "
            f"to {customer_name} - {total_pizzas} pizza(s) cooked by {cooked_by}"
        )
        
        # Publish delivery_started event
        delivery_started_event = create_order_event_message(
            order_id=order_id,
            event_type=EventType.DELIVERY_STARTED.value,
            actor=self.worker_id,
            order_number=order_number,
            customer_name=customer_name,
            total_pizzas=total_pizzas,
            metadata={}
        )
        self._publish_message(
            topic=TOPIC_ORDER_EVENTS,
            key=order_id,
            value=delivery_started_event
        )
        
        # Calculate random delivery time within configured range
        delivery_time = random.uniform(DELIVERY_TIME_MIN, DELIVERY_TIME_MAX)
        
        # Simulate delivery
        start_time = time.time()
        time.sleep(delivery_time)
        actual_delivery_time = time.time() - start_time
        
        self.logger.info(
            f"Delivered order #{order_number} to {customer_name} in {actual_delivery_time:.2f}s"
        )
        
        # Publish delivery_completed event
        delivery_completed_event = create_order_event_message(
            order_id=order_id,
            event_type=EventType.DELIVERY_COMPLETED.value,
            actor=self.worker_id,
            order_number=order_number,
            customer_name=customer_name,
            total_pizzas=total_pizzas,
            metadata={"delivery_duration_seconds": actual_delivery_time}
        )
        self._publish_message(
            topic=TOPIC_ORDER_EVENTS,
            key=order_id,
            value=delivery_completed_event
        )
        
        self.logger.debug(f"Published delivery events for order #{order_number}")
    
    def run(self) -> None:
        """
        Run the delivery worker consumer loop.
        
        Continuously polls for cooked pizzas and delivers them until stopped.
        """
        self.running = True
        self.logger.info(
            f"Delivery Worker '{self.worker_id}' ({self.worker_type}) is ready to deliver pizzas!"
        )
        
        try:
            while self.running:
                # Poll for messages with 1 second timeout
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize the cooked pizza message
                    cooked_pizza_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Deliver the order
                    self._deliver_order(cooked_pizza_data)
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {e}")
                except KeyError as e:
                    self.logger.error(f"Missing required field in cooked pizza: {e}")
                except Exception as e:
                    self.logger.error(f"Error delivering order: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            self.logger.info("Received KeyboardInterrupt (Ctrl+C) - initiating graceful shutdown")
        except Exception as e:
            self.logger.error(f"Unexpected error in delivery worker: {e}", exc_info=True)
        finally:
            # Always ensure graceful shutdown
            self.shutdown()
    
    def shutdown(self) -> None:
        """Clean shutdown of the delivery worker."""
        if not self.running:
            return  # Already shut down
            
        self.logger.info(f"Shutting down Delivery Worker '{self.worker_id}'")
        
        # Set running to False FIRST to exit the poll loop
        self.running = False
        
        # Give the poll loop time to exit gracefully (1-2 seconds max)
        time.sleep(1.5)
        
        try:
            # Flush producer to ensure all messages are sent
            if self.producer:
                self.logger.debug("Flushing producer...")
                remaining = self.producer.flush(timeout=5.0)
                if remaining > 0:
                    self.logger.warning(f"{remaining} messages not delivered before shutdown")
        except Exception as e:
            self.logger.error(f"Error flushing producer: {e}")
        
        try:
            # Close consumer (now safe as poll loop has exited)
            if self.consumer:
                self.logger.debug("Closing consumer...")
                self.consumer.close()
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")
        
        self.logger.info(f"Delivery Worker '{self.worker_id}' shut down successfully")


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    print("\nReceived interrupt signal, shutting down...")
    sys.exit(0)


def main():
    """Main entry point for the delivery worker consumer."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: python -m src.consumers.delivery_worker <worker_id> <worker_type>")
        print("Example: python -m src.consumers.delivery_worker driver-1 driver")
        print("Example: python -m src.consumers.delivery_worker frontdesk frontdesk")
        sys.exit(1)
    
    worker_id = sys.argv[1]
    worker_type = sys.argv[2]
    
    # Create and run delivery worker
    delivery_worker = DeliveryWorker(worker_id, worker_type)
    delivery_worker.run()


if __name__ == "__main__":
    main()
