"""
Bookkeeper consumer for the pizza shop demo system.

Consumes all order lifecycle events from the order-events topic,
tracks statistics, and displays a real-time dashboard.
"""

import sys
import json
import time
import signal
import os
from typing import Optional, Dict, Any
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.config import (
    get_consumer_config,
    TOPIC_ORDER_EVENTS,
    LOG_LEVEL,
)
from src.models import OrderEventMessage, EventType
from src.utils import setup_logging, create_kafka_consumer


class Bookkeeper:
    """
    Bookkeeper consumer that tracks all order lifecycle events and displays a dashboard.
    
    Consumes from order-events topic and maintains real-time statistics
    about order processing.
    """
    
    def __init__(self):
        """Initialize the bookkeeper."""
        self.logger = setup_logging(LOG_LEVEL, "Bookkeeper")
        self.running = False
        
        # Statistics
        self.orders_created = 0
        self.orders_cooking_started = 0
        self.orders_cooking_completed = 0
        self.orders_delivery_started = 0
        self.orders_delivery_completed = 0
        
        self.total_cooking_time = 0.0
        self.total_delivery_time = 0.0
        
        # Track individual order states
        self.order_states: Dict[str, Dict[str, Any]] = {}
        
        # Dashboard display settings
        self.last_dashboard_update = 0
        self.dashboard_update_interval = 2.0  # seconds
        
        # Create consumer (not part of a consumer group - unique group ID)
        consumer_config = get_consumer_config(
            group_id=f"bookkeeper-{int(time.time())}",  # Unique group to read all messages
            client_id="bookkeeper"
        )
        # Start from beginning to catch all events
        consumer_config["auto.offset.reset"] = "earliest"
        self.consumer = create_kafka_consumer(consumer_config)
        
        # Subscribe to order-events topic
        self.consumer.subscribe([TOPIC_ORDER_EVENTS])
        
        self.logger.info("Bookkeeper initialized")
        self.logger.info(f"Subscribed to topic: {TOPIC_ORDER_EVENTS}")
        self.logger.info("Starting real-time order tracking dashboard...")
        
        # Initial dashboard display
        self._display_dashboard(force=True)
    
    def _process_event(self, event: OrderEventMessage) -> None:
        """
        Process an order event and update statistics.
        
        Args:
            event: Order event message to process
        """
        order_id = event["order_id"]
        event_type = event["event_type"]
        order_number = event["order_number"]
        customer_name = event["customer_name"]
        metadata = event.get("metadata", {})
        
        # Initialize order state if needed
        if order_id not in self.order_states:
            self.order_states[order_id] = {
                "order_number": order_number,
                "customer_name": customer_name,
                "current_state": "unknown",
                "created_at": None,
                "cooking_started_at": None,
                "cooking_completed_at": None,
                "delivery_started_at": None,
                "delivery_completed_at": None,
            }
        
        order_state = self.order_states[order_id]
        timestamp = event["timestamp"]
        
        # Update state based on event type
        if event_type == EventType.ORDER_CREATED.value:
            self.orders_created += 1
            order_state["created_at"] = timestamp
            order_state["current_state"] = "created"
            self.logger.info(f"Order #{order_number} created for {customer_name}")
        
        elif event_type == EventType.COOKING_STARTED.value:
            self.orders_cooking_started += 1
            order_state["cooking_started_at"] = timestamp
            order_state["current_state"] = "cooking"
            self.logger.debug(f"Order #{order_number} cooking started")
        
        elif event_type == EventType.COOKING_COMPLETED.value:
            self.orders_cooking_completed += 1
            order_state["cooking_completed_at"] = timestamp
            order_state["current_state"] = "cooked"
            
            # Track cooking time
            cooking_duration = metadata.get("cooking_duration_seconds", 0)
            if cooking_duration:
                self.total_cooking_time += cooking_duration
            
            self.logger.debug(
                f"Order #{order_number} cooking completed in {cooking_duration:.2f}s"
            )
        
        elif event_type == EventType.DELIVERY_STARTED.value:
            self.orders_delivery_started += 1
            order_state["delivery_started_at"] = timestamp
            order_state["current_state"] = "delivering"
            self.logger.debug(f"Order #{order_number} delivery started")
        
        elif event_type == EventType.DELIVERY_COMPLETED.value:
            self.orders_delivery_completed += 1
            order_state["delivery_completed_at"] = timestamp
            order_state["current_state"] = "delivered"
            
            # Track delivery time
            delivery_duration = metadata.get("delivery_duration_seconds", 0)
            if delivery_duration:
                self.total_delivery_time += delivery_duration
            
            self.logger.info(
                f"Order #{order_number} delivered to {customer_name} "
                f"in {delivery_duration:.2f}s"
            )
    
    def _calculate_statistics(self) -> Dict[str, Any]:
        """
        Calculate current statistics.
        
        Returns:
            Dictionary containing current statistics
        """
        # Count orders in each state
        orders_in_progress = sum(
            1 for order in self.order_states.values()
            if order["current_state"] not in ["delivered", "unknown"]
        )
        
        orders_cooking = sum(
            1 for order in self.order_states.values()
            if order["current_state"] == "cooking"
        )
        
        orders_delivering = sum(
            1 for order in self.order_states.values()
            if order["current_state"] == "delivering"
        )
        
        # Calculate averages
        avg_cooking_time = (
            self.total_cooking_time / self.orders_cooking_completed
            if self.orders_cooking_completed > 0
            else 0
        )
        
        avg_delivery_time = (
            self.total_delivery_time / self.orders_delivery_completed
            if self.orders_delivery_completed > 0
            else 0
        )
        
        return {
            "orders_created": self.orders_created,
            "orders_cooking": orders_cooking,
            "orders_cooked": self.orders_cooking_completed,
            "orders_delivering": orders_delivering,
            "orders_delivered": self.orders_delivery_completed,
            "orders_in_progress": orders_in_progress,
            "avg_cooking_time": avg_cooking_time,
            "avg_delivery_time": avg_delivery_time,
        }
    
    def _display_dashboard(self, force: bool = False) -> None:
        """
        Display the real-time dashboard.
        
        Args:
            force: If True, display immediately regardless of update interval
        """
        current_time = time.time()
        
        # Check if it's time to update
        if not force and (current_time - self.last_dashboard_update) < self.dashboard_update_interval:
            return
        
        self.last_dashboard_update = current_time
        
        # Calculate statistics
        stats = self._calculate_statistics()
        
        # Clear screen (works on Unix and Windows)
        os.system('clear' if os.name == 'posix' else 'cls')
        
        # Display dashboard
        print("=" * 70)
        print("🍕 PIZZA SHOP - REAL-TIME ORDER DASHBOARD 🍕".center(70))
        print("=" * 70)
        print()
        
        print("📊 ORDER STATISTICS:")
        print(f"  Total Orders Received:    {stats['orders_created']:>4}")
        print(f"  Orders Being Cooked:      {stats['orders_cooking']:>4}")
        print(f"  Orders Cooked:            {stats['orders_cooked']:>4}")
        print(f"  Orders Being Delivered:   {stats['orders_delivering']:>4}")
        print(f"  Orders Delivered:         {stats['orders_delivered']:>4}")
        print(f"  Orders In Progress:       {stats['orders_in_progress']:>4}")
        print()
        
        print("⏱️  PERFORMANCE METRICS:")
        print(f"  Average Cooking Time:     {stats['avg_cooking_time']:>6.2f}s")
        print(f"  Average Delivery Time:    {stats['avg_delivery_time']:>6.2f}s")
        print()
        
        # Calculate completion percentage
        if stats['orders_created'] > 0:
            completion_pct = (stats['orders_delivered'] / stats['orders_created']) * 100
            print(f"📈 COMPLETION: {completion_pct:>5.1f}%")
            
            # Progress bar
            bar_width = 50
            filled = int(bar_width * stats['orders_delivered'] / stats['orders_created'])
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"  [{bar}]")
        print()
        
        print(f"🕐 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print()
        print("Press Ctrl+C to stop the bookkeeper...")
        print()
    
    def run(self) -> None:
        """
        Run the bookkeeper consumer loop.
        
        Continuously polls for events and updates the dashboard.
        """
        self.running = True
        self.logger.info("Bookkeeper is tracking orders!")
        
        try:
            while self.running:
                # Poll for messages with 0.1 second timeout for responsive dashboard updates
                msg = self.consumer.poll(timeout=0.1)
                
                if msg is None:
                    # No message - update dashboard if needed
                    self._display_dashboard()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        self._display_dashboard()
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Deserialize the event message
                    event_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Process the event
                    self._process_event(event_data)
                    
                    # Update dashboard
                    self._display_dashboard()
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to decode message: {e}")
                except KeyError as e:
                    self.logger.error(f"Missing required field in event: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing event: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            self.logger.info("Received KeyboardInterrupt (Ctrl+C) - initiating graceful shutdown")
        except Exception as e:
            self.logger.error(f"Unexpected error in bookkeeper: {e}", exc_info=True)
        finally:
            # Always ensure graceful shutdown
            self.shutdown()
    
    def shutdown(self) -> None:
        """Clean shutdown of the bookkeeper."""
        if not self.running:
            return  # Already shut down
            
        self.logger.info("Shutting down Bookkeeper")
        
        # Set running to False FIRST to exit the poll loop
        self.running = False
        
        # Give the poll loop time to exit gracefully (1-2 seconds max)
        time.sleep(1.5)
        
        try:
            # Display final statistics
            print("\n" + "=" * 70)
            print("📊 FINAL STATISTICS".center(70))
            print("=" * 70)
            
            stats = self._calculate_statistics()
            print(f"\nTotal Orders Processed: {stats['orders_created']}")
            print(f"Total Orders Delivered: {stats['orders_delivered']}")
            print(f"Average Cooking Time: {stats['avg_cooking_time']:.2f}s")
            print(f"Average Delivery Time: {stats['avg_delivery_time']:.2f}s")
            
            if stats['orders_created'] > 0:
                completion_pct = (stats['orders_delivered'] / stats['orders_created']) * 100
                print(f"Completion Rate: {completion_pct:.1f}%")
            
            print("=" * 70)
        except Exception as e:
            self.logger.error(f"Error displaying final statistics: {e}")
        
        try:
            # Close consumer (now safe as poll loop has exited)
            if self.consumer:
                self.logger.debug("Closing consumer...")
                self.consumer.close()
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")
        
        self.logger.info("Bookkeeper shut down successfully")


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    print("\nReceived interrupt signal, shutting down...")
    sys.exit(0)


def main():
    """Main entry point for the bookkeeper consumer."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run bookkeeper
    bookkeeper = Bookkeeper()
    bookkeeper.run()


if __name__ == "__main__":
    main()
