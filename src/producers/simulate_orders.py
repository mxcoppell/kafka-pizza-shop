"""
Order simulation orchestrator for the pizza shop demo system.

Runs both web and frontdesk producers concurrently to simulate realistic
order flow with 200 total orders (120 web, 80 frontdesk).
"""

import time
import sys
import threading
from typing import Dict, Any

from src.config import WEB_ORDERS, FRONTDESK_ORDERS, TOTAL_ORDERS
from src.producers.web_producer import run_web_producer
from src.producers.frontdesk_producer import run_frontdesk_producer
from src.utils import setup_logging


class ProducerThread(threading.Thread):
    """Thread wrapper for running a producer function."""
    
    def __init__(self, producer_func, *args, **kwargs):
        """
        Initialize producer thread.
        
        Args:
            producer_func: Producer function to run
            *args: Arguments for producer function
            **kwargs: Keyword arguments for producer function
        """
        super().__init__()
        self.producer_func = producer_func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.exception = None
    
    def run(self):
        """Run the producer function."""
        try:
            self.result = self.producer_func(*self.args, **self.kwargs)
        except Exception as e:
            self.exception = e


def simulate_orders(
    web_orders: int = WEB_ORDERS,
    frontdesk_orders: int = FRONTDESK_ORDERS,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Simulate pizza orders from both web and frontdesk sources concurrently.
    
    Args:
        web_orders: Number of orders from web interface (default: 120)
        frontdesk_orders: Number of orders from frontdesk (default: 80)
        verbose: Enable verbose logging
        
    Returns:
        Dictionary with combined statistics
    """
    logger = setup_logging(component_name="simulate_orders")
    
    total_orders = web_orders + frontdesk_orders
    
    logger.info("=" * 70)
    logger.info("PIZZA SHOP DEMO - ORDER SIMULATION")
    logger.info("=" * 70)
    logger.info(f"Total orders to generate: {total_orders}")
    logger.info(f"  - Web orders: {web_orders} ({web_orders/total_orders*100:.1f}%)")
    logger.info(f"  - Frontdesk orders: {frontdesk_orders} ({frontdesk_orders/total_orders*100:.1f}%)")
    logger.info("=" * 70)
    logger.info("Starting concurrent producers...")
    logger.info("")
    
    # Record start time
    start_time = time.time()
    
    # Create producer threads
    # Both start from order number 1 and will produce interleaved orders
    web_thread = ProducerThread(
        run_web_producer,
        num_orders=web_orders,
        start_order_number=1,
        verbose=verbose
    )
    
    frontdesk_thread = ProducerThread(
        run_frontdesk_producer,
        num_orders=frontdesk_orders,
        start_order_number=web_orders + 1,  # Start after web orders
        verbose=verbose
    )
    
    # Start both threads
    web_thread.start()
    frontdesk_thread.start()
    
    # Wait for both to complete
    logger.info("Waiting for producers to complete...")
    web_thread.join()
    frontdesk_thread.join()
    
    # Calculate total elapsed time
    elapsed_time = time.time() - start_time
    
    # Check for exceptions
    if web_thread.exception:
        logger.error(f"Web producer failed: {web_thread.exception}")
    if frontdesk_thread.exception:
        logger.error(f"Frontdesk producer failed: {frontdesk_thread.exception}")
    
    # Collect results
    web_stats = web_thread.result or {"orders_published": 0, "events_published": 0, "errors": 1}
    frontdesk_stats = frontdesk_thread.result or {"orders_published": 0, "events_published": 0, "errors": 1}
    
    # Calculate combined statistics
    total_orders_published = web_stats.get("orders_published", 0) + frontdesk_stats.get("orders_published", 0)
    total_events_published = web_stats.get("events_published", 0) + frontdesk_stats.get("events_published", 0)
    total_errors = web_stats.get("errors", 0) + frontdesk_stats.get("errors", 0)
    
    # Print comprehensive summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("ORDER SIMULATION SUMMARY")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Web Interface:")
    logger.info(f"  Orders published: {web_stats.get('orders_published', 0)}")
    logger.info(f"  Events published: {web_stats.get('events_published', 0)}")
    logger.info(f"  Errors: {web_stats.get('errors', 0)}")
    if "elapsed_time_seconds" in web_stats:
        logger.info(f"  Time: {web_stats['elapsed_time_seconds']:.2f}s")
    
    logger.info("")
    logger.info("Frontdesk:")
    logger.info(f"  Orders published: {frontdesk_stats.get('orders_published', 0)}")
    logger.info(f"  Events published: {frontdesk_stats.get('events_published', 0)}")
    logger.info(f"  Errors: {frontdesk_stats.get('errors', 0)}")
    if "elapsed_time_seconds" in frontdesk_stats:
        logger.info(f"  Time: {frontdesk_stats['elapsed_time_seconds']:.2f}s")
    
    logger.info("")
    logger.info("Combined Totals:")
    logger.info(f"  Total orders published: {total_orders_published} / {total_orders}")
    logger.info(f"  Total events published: {total_events_published}")
    logger.info(f"  Total errors: {total_errors}")
    logger.info(f"  Total elapsed time: {elapsed_time:.2f} seconds")
    
    if elapsed_time > 0:
        throughput = total_orders_published / elapsed_time
        logger.info(f"  Overall throughput: {throughput:.2f} orders/second")
    
    # Success rate
    if total_orders > 0:
        success_rate = (total_orders_published / total_orders) * 100
        logger.info(f"  Success rate: {success_rate:.1f}%")
    
    logger.info("=" * 70)
    
    # Return combined statistics
    return {
        "web_stats": web_stats,
        "frontdesk_stats": frontdesk_stats,
        "total_orders_published": total_orders_published,
        "total_events_published": total_events_published,
        "total_errors": total_errors,
        "elapsed_time_seconds": elapsed_time,
        "throughput": total_orders_published / elapsed_time if elapsed_time > 0 else 0,
        "success_rate": (total_orders_published / total_orders * 100) if total_orders > 0 else 0,
    }


def main():
    """Main entry point for standalone execution."""
    logger = setup_logging(component_name="simulate_orders_main")
    
    # Parse command line arguments
    web_orders = WEB_ORDERS
    frontdesk_orders = FRONTDESK_ORDERS
    
    if len(sys.argv) > 1:
        try:
            web_orders = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid number of web orders: {sys.argv[1]}")
            print("Usage: python -m src.producers.simulate_orders [web_orders] [frontdesk_orders]")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            frontdesk_orders = int(sys.argv[2])
        except ValueError:
            logger.error(f"Invalid number of frontdesk orders: {sys.argv[2]}")
            print("Usage: python -m src.producers.simulate_orders [web_orders] [frontdesk_orders]")
            sys.exit(1)
    
    # Run simulation
    try:
        stats = simulate_orders(web_orders=web_orders, frontdesk_orders=frontdesk_orders)
        
        # Exit with error if there were failures
        if stats["total_errors"] > 0:
            logger.warning("Simulation completed with errors")
            sys.exit(1)
        else:
            logger.info("Simulation completed successfully")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Simulation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
