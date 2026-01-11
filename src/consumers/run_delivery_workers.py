"""
Runner script for starting multiple delivery worker instances concurrently.

This script starts 3 delivery worker instances (frontdesk, driver-1, driver-2)
using threading for parallel execution. Each worker runs in its own thread and
processes cooked pizzas from the cooked-pizzas topic.
"""

import sys
import threading
import time
import signal
from typing import List, Tuple

from src.consumers.delivery_worker import DeliveryWorker
from src.utils import setup_logging


class DeliveryWorkersRunner:
    """Runner for managing multiple delivery worker instances."""
    
    def __init__(self):
        """Initialize the delivery workers runner."""
        self.workers: List[DeliveryWorker] = []
        self.threads: List[threading.Thread] = []
        self.logger = setup_logging(component_name="DeliveryWorkersRunner")
        self.running = False
        
        # Define worker configurations (worker_id, worker_type)
        self.worker_configs: List[Tuple[str, str]] = [
            ("frontdesk", "frontdesk"),
            ("driver-1", "driver"),
            ("driver-2", "driver"),
        ]
    
    def start(self) -> None:
        """Start all delivery worker instances in separate threads."""
        self.logger.info("=" * 70)
        self.logger.info("DELIVERY WORKERS - STARTING PARALLEL INSTANCES")
        self.logger.info("=" * 70)
        self.logger.info(f"Starting {len(self.worker_configs)} delivery worker instances...")
        self.logger.info("")
        
        # Create and start delivery worker threads
        for worker_id, worker_type in self.worker_configs:
            try:
                # Create delivery worker instance
                worker = DeliveryWorker(worker_id, worker_type)
                self.workers.append(worker)
                
                # Create thread with daemon=True for clean shutdown
                thread = threading.Thread(
                    target=worker.run,
                    name=f"DeliveryWorker-{worker_id}",
                    daemon=True
                )
                self.threads.append(thread)
                
                # Start the thread
                thread.start()
                self.logger.info(f"✓ Started {worker_id} ({worker_type})")
                
            except Exception as e:
                self.logger.error(f"Failed to start {worker_id}: {e}")
        
        self.running = True
        
        self.logger.info("")
        self.logger.info(f"All {len(self.threads)} delivery workers are running!")
        self.logger.info("Press Ctrl+C to stop all delivery workers")
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
        """Gracefully shutdown all delivery worker instances."""
        if not self.running:
            return
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("SHUTTING DOWN ALL DELIVERY WORKERS")
        self.logger.info("=" * 70)
        
        self.running = False
        
        # Call shutdown on each worker
        for worker in self.workers:
            try:
                worker.shutdown()
            except Exception as e:
                self.logger.error(f"Error shutting down {worker.worker_id}: {e}")
        
        # Wait for threads to finish (with timeout)
        self.logger.info("Waiting for threads to complete...")
        for thread in self.threads:
            try:
                thread.join(timeout=3.0)
                if thread.is_alive():
                    self.logger.warning(f"Thread {thread.name} did not complete in time")
            except Exception as e:
                self.logger.error(f"Error joining thread {thread.name}: {e}")
        
        self.logger.info("All delivery workers shut down successfully")
        self.logger.info("=" * 70)


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    print("\nReceived interrupt signal, shutting down...")
    sys.exit(0)


def main():
    """Main entry point for running multiple delivery workers."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run the delivery workers runner
    runner = DeliveryWorkersRunner()
    
    try:
        runner.start()
        runner.wait()
    except Exception as e:
        print(f"Error running delivery workers: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
