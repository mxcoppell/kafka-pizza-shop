"""
Web interface producer for the pizza shop demo system.

Simulates orders coming from a web interface, generating 120 orders
with a burst timing pattern for realistic demo behavior.
"""

import time
import random
import sys
from typing import Optional

from src.config import WEB_ORDERS, ORDER_TIMING_PATTERNS
from src.producers.order_producer import OrderProducer
from src.utils import setup_logging


def get_order_delay(order_number: int, total_orders: int) -> float:
    """
    Calculate delay before next order based on burst timing pattern.
    
    Args:
        order_number: Current order number (1-indexed)
        total_orders: Total number of orders to generate
        
    Returns:
        Delay in seconds before next order
    """
    # Calculate proportional ranges for this producer
    # Web gets 60% of orders, so scale the timing pattern
    initial_burst_end = int(total_orders * 0.25)  # First 25%
    steady_flow_end = int(total_orders * 0.75)    # Middle 50%
    # Final 25% is final rush
    
    if order_number <= initial_burst_end:
        # Initial burst: rapid fire
        delay_min = 0.05
        delay_max = 0.1
    elif order_number <= steady_flow_end:
        # Steady flow: moderate pace
        delay_min = 0.1
        delay_max = 0.3
    else:
        # Final rush: rapid fire again
        delay_min = 0.05
        delay_max = 0.1
    
    # Add slight randomization to prevent artificial synchronization
    return random.uniform(delay_min, delay_max)


def run_web_producer(
    num_orders: int = WEB_ORDERS,
    start_order_number: int = 1,
    verbose: bool = True
) -> dict:
    """
    Run the web interface producer.
    
    Args:
        num_orders: Number of orders to generate (default: 120)
        start_order_number: Starting order number (default: 1)
        verbose: Enable verbose logging
        
    Returns:
        Dictionary with statistics
    """
    logger = setup_logging(component_name="web_producer")
    
    logger.info("=" * 60)
    logger.info("PIZZA SHOP DEMO - WEB INTERFACE PRODUCER")
    logger.info("=" * 60)
    logger.info(f"Total orders to generate: {num_orders}")
    logger.info(f"Starting from order number: {start_order_number}")
    logger.info(f"Source: web")
    logger.info("=" * 60)
    
    # Create producer with context manager
    try:
        with OrderProducer(source="web") as producer:
            start_time = time.time()
            
            # Generate orders
            for i in range(num_orders):
                order_number = start_order_number + i
                
                # Publish order
                order = producer.publish_order(order_number)
                
                if order is None:
                    logger.error(f"Failed to publish order #{order_number}")
                    continue
                
                # Calculate and apply delay before next order (except for last)
                if i < num_orders - 1:
                    delay = get_order_delay(i + 1, num_orders)
                    time.sleep(delay)
            
            # Get final statistics
            stats = producer.get_statistics()
            elapsed_time = time.time() - start_time
            
            # Add timing information
            stats["elapsed_time_seconds"] = elapsed_time
            stats["orders_per_second"] = stats["orders_published"] / elapsed_time if elapsed_time > 0 else 0
            
            # Print summary
            logger.info("=" * 60)
            logger.info("WEB PRODUCER SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Orders published: {stats['orders_published']}")
            logger.info(f"Events published: {stats['events_published']}")
            logger.info(f"Errors: {stats['errors']}")
            logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
            logger.info(f"Throughput: {stats['orders_per_second']:.2f} orders/second")
            logger.info("=" * 60)
            
            return stats
            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt (Ctrl+C) - web producer shutting down gracefully")
        return {"error": "interrupted", "reason": "KeyboardInterrupt"}
        
    except Exception as e:
        logger.error(f"Web producer failed with exception: {e}", exc_info=True)
        return {"error": str(e)}


def main():
    """Main entry point for standalone execution."""
    # Parse command line arguments
    num_orders = WEB_ORDERS
    start_order = 1
    
    if len(sys.argv) > 1:
        try:
            num_orders = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number of orders: {sys.argv[1]}")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            start_order = int(sys.argv[2])
        except ValueError:
            print(f"Invalid start order number: {sys.argv[2]}")
            sys.exit(1)
    
    # Run producer
    stats = run_web_producer(num_orders=num_orders, start_order_number=start_order)
    
    # Exit with appropriate code
    if "error" in stats:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
