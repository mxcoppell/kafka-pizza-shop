"""
Pizza Maker consumer for the pizza shop demo system.

Consumes pizza orders from the pizza-orders topic, simulates cooking time,
and publishes cooked pizzas to the cooked-pizzas topic along with cooking events.
"""

import sys
import json
import time
import signal
from typing import Optional, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from src.config import (
    get_consumer_config,
    get_producer_config,
    TOPIC_PIZZA_ORDERS,
    TOPIC_COOKED_PIZZAS,
    TOPIC_ORDER_EVENTS,
    CONSUMER_GROUP_PIZZA_MAKERS,
    COOKING_TIME_MULTIPLIER,
    LOG_LEVEL,
)
from src.models import (
    OrderMessage,
    create_cooked_pizza_message,
    create_order_event_message,
    calculate_cooking_time,
    EventType,
)
from src.utils import setup_logging, create_kafka_consumer, create_kafka_producer


class PizzaMaker:
    """
    Pizza Maker consumer that processes pizza orders.
    
    Consumes from pizza-orders topic, simulates cooking time based on
    pizza types, and publishes results to cooked-pizzas and order-events topics.
    """
    
    def __init__(self, maker_id: str):
        """
        Initialize the pizza maker.
        
        Args:
            maker_id: Unique identifier for this pizza maker (e.g., "maker-1")
        """
        self.maker_id = maker_id
        self.logger = setup_logging(LOG_LEVEL, f"PizzaMaker-{maker_id}")
        self.running = False
        
        # Create consumer for pizza-orders topic
        consumer_config = get_consumer_config(
            group_id=CONSUMER_GROUP_PIZZA_MAKERS,
            client_id=f"pizza-maker-{maker_id}"
        )
        self.consumer = create_kafka_consumer(consumer_config)
        
        # Create producer for cooked-pizzas and order-events topics
        producer_config = get_producer_config()
        producer_config["client.id"] = f"pizza-maker-{maker_id}"
        self.producer = create_kafka_producer(producer_config)
        
        # Subscribe to pizza-orders topic
        self.consumer.subscribe([TOPIC_PIZZA_ORDERS])
        
        self.logger.info(f"Pizza Maker '{maker_id}' initialized")
        self.logger.info(f"Subscribed to topic: {TOPIC_PIZZA_ORDERS}")
        self.logger.info(f"Consumer group: {CONSUMER_GROUP_PIZZA_MAKERS}")
    
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
    
    def _process_order(self, order: OrderMessage) -> None:
        """
        Process a pizza order by cooking it and publishing results.
        
        Args:
            order: Pizza order message to process
        """
        order_id = order["order_id"]
        order_number = order["order_number"]
        customer_name = order["customer_name"]
        total_pizzas = order["total_pizzas"]
        
        self.logger.info(
            f"Started cooking order #{order_number} (ID: {order_id[:8]}...) "
            f"for {customer_name} - {total_pizzas} pizza(s)"
        )
        
        # Publish cooking_started event
        cooking_started_event = create_order_event_message(
            order_id=order_id,
            event_type=EventType.COOKING_STARTED.value,
            actor=self.maker_id,
            order_number=order_number,
            customer_name=customer_name,
            total_pizzas=total_pizzas,
            metadata={}
        )
        self._publish_message(
            topic=TOPIC_ORDER_EVENTS,
            key=order_id,
            value=cooking_started_event
        )
        
        # Calculate cooking time
        cooking_time = calculate_cooking_time(
            order["pizzas"],
            time_multiplier=COOKING_TIME_MULTIPLIER
        )
        
        # Simulate cooking
        start_time = time.time()
        time.sleep(cooking_time)
        actual_cooking_time = time.time() - start_time
        
        self.logger.info(
            f"Finished cooking order #{order_number} in {actual_cooking_time:.2f}s"
        )
        
        # Create and publish cooked pizza message
        cooked_pizza = create_cooked_pizza_message(
            original_order=order,
            cooked_by=self.maker_id,
            cooking_duration_seconds=actual_cooking_time
        )
        self._publish_message(
            topic=TOPIC_COOKED_PIZZAS,
            key=order_id,
            value=cooked_pizza
        )
        
        # Publish cooking_completed event
        cooking_completed_event = create_order_event_message(
            order_id=order_id,
            event_type=EventType.COOKING_COMPLETED.value,
            actor=self.maker_id,
            order_number=order_number,
            customer_name=customer_name,
            total_pizzas=total_pizzas,
            metadata={"cooking_duration_seconds": actual_cooking_time}
        )
        self._publish_message(
            topic=TOPIC_ORDER_EVENTS,
            key=order_id,
            value=cooking_completed_event
        )
        
        self.logger.debug(f"Published cooked pizza and events for order #{order_number}")
    
    def run(self) -> None:
        """
        Run the pizza maker consumer loop.
        
        Continuously polls for new orders and processes them until stopped.
        """
        self.running = True
        self.logger.info(f"Pizza Maker '{self.maker_id}' is ready to cook pizzas!")
        
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
                    # Deserialize the order message
                    order_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Process the order
                    self._process_order(order_data)
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {e}")
                except KeyError as e:
                    self.logger.error(f"Missing required field in order: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing order: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            self.logger.info("Received KeyboardInterrupt (Ctrl+C) - initiating graceful shutdown")
        except Exception as e:
            self.logger.error(f"Unexpected error in pizza maker: {e}", exc_info=True)
        finally:
            # Always ensure graceful shutdown
            self.shutdown()
    
    def shutdown(self) -> None:
        """Clean shutdown of the pizza maker."""
        if not self.running:
            return  # Already shut down
            
        self.logger.info(f"Shutting down Pizza Maker '{self.maker_id}'")
        
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
        
        self.logger.info(f"Pizza Maker '{self.maker_id}' shut down successfully")


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    print("\nReceived interrupt signal, shutting down...")
    sys.exit(0)


def main():
    """Main entry point for the pizza maker consumer."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python -m src.consumers.pizza_maker <maker_id>")
        print("Example: python -m src.consumers.pizza_maker maker-1")
        sys.exit(1)
    
    maker_id = sys.argv[1]
    
    # Create and run pizza maker
    pizza_maker = PizzaMaker(maker_id)
    pizza_maker.run()


if __name__ == "__main__":
    main()
