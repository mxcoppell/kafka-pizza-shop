"""
Runner script for starting multiple pizza maker instances concurrently.

This script starts 3 pizza maker instances (maker-1, maker-2, maker-3) using
threading for parallel execution. Each maker runs in its own thread and
processes orders from the pizza-orders topic.
"""

import sys
import threading
import time
import signal
from typing import List

from src.consumers.pizza_maker import PizzaMaker
from src.utils import setup_logging


class PizzaMakersRunner:
    """Runner for managing multiple pizza maker instances."""
    
    def __init__(self, num_makers: int = 3):
        """
        Initialize the pizza makers runner.
        
        Args:
            num_makers: Number of pizza maker instances to create (default: 3)
        """
        self.num_makers = num_makers
        self.makers: List[PizzaMaker] = []
        self.threads: List[threading.Thread] = []
        self.logger = setup_logging(component_name="PizzaMakersRunner")
        self.running = False
    
    def start(self) -> None:
        """Start all pizza maker instances in separate threads."""
        self.logger.info("=" * 70)
        self.logger.info("PIZZA MAKERS - STARTING PARALLEL INSTANCES")
        self.logger.info("=" * 70)
        self.logger.info(f"Starting {self.num_makers} pizza maker instances...")
        self.logger.info("")
        
        # Create and start pizza maker threads
        for i in range(1, self.num_makers + 1):
            maker_id = f"maker-{i}"
            
            try:
                # Create pizza maker instance
                maker = PizzaMaker(maker_id)
                self.makers.append(maker)
                
                # Create thread with daemon=True for clean shutdown
                thread = threading.Thread(
                    target=maker.run,
                    name=f"PizzaMaker-{maker_id}",
                    daemon=True
                )
                self.threads.append(thread)
                
                # Start the thread
                thread.start()
                self.logger.info(f"✓ Started {maker_id}")
                
            except Exception as e:
                self.logger.error(f"Failed to start {maker_id}: {e}")
        
        self.running = True
        
        self.logger.info("")
        self.logger.info(f"All {len(self.threads)} pizza makers are running!")
        self.logger.info("Press Ctrl+C to stop all pizza makers")
        self.logger.info("=" * 70)
        self.logger.info("")
    
    def wait(self) -> None:
        """Wait for all threads to complete or until interrupted."""
        try:
            # Keep main thread alive while worker threads are running
            while self.running and any(t.is_alive() for t in self.threads):
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.logger.info("\nReceived KeyboardInterrupt (Ctrl+C) - initiating graceful shutdown")
            self.shutdown()
    
    def shutdown(self) -> None:
        """Gracefully shutdown all pizza maker instances."""
        if not self.running:
            return
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SHUTTING DOWN ALL PIZZA MAKERS")
        self.logger.info("=" * 70)
        
        self.running = False
        
        # Call shutdown on each maker
        for maker in self.makers:
            try:
                maker.shutdown()
            except Exception as e:
                self.logger.error(f"Error shutting down {maker.maker_id}: {e}")
        
        # Wait for threads to finish (with timeout)
        self.logger.info("Waiting for threads to complete...")
        for thread in self.threads:
            try:
                thread.join(timeout=3.0)
                if thread.is_alive():
                    self.logger.warning(f"Thread {thread.name} did not complete in time")
            except Exception as e:
                self.logger.error(f"Error joining thread {thread.name}: {e}")
        
        self.logger.info("All pizza makers shut down successfully")
        self.logger.info("=" * 70)


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    print("\nReceived interrupt signal, shutting down...")
    sys.exit(0)


def main():
    """Main entry point for running multiple pizza makers."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    num_makers = 3
    if len(sys.argv) > 1:
        try:
            num_makers = int(sys.argv[1])
            if num_makers < 1 or num_makers > 10:
                print("Number of makers must be between 1 and 10")
                sys.exit(1)
        except ValueError:
            print("Usage: python -m src.consumers.run_pizza_makers [num_makers]")
            print("Example: python -m src.consumers.run_pizza_makers 3")
            sys.exit(1)
    
    # Create and run the pizza makers runner
    runner = PizzaMakersRunner(num_makers=num_makers)
    
    try:
        runner.start()
        runner.wait()
    except Exception as e:
        print(f"Error running pizza makers: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
