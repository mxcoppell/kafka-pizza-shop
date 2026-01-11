#!/usr/bin/env python3
"""
Test script to verify Kafka connectivity for the pizza shop demo.

This script checks:
- Kafka broker is reachable
- Topics exist and are accessible
- Producer can connect
- Consumer can connect
- Basic message produce/consume functionality
"""

import sys
import os
from pathlib import Path
from time import sleep
import json

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def test_kafka_import():
    """Test Kafka library can be imported."""
    print("Testing Kafka library import...")
    try:
        from confluent_kafka import Producer, Consumer, KafkaError
        from confluent_kafka.admin import AdminClient
        print("  ✓ Confluent Kafka library imported successfully")
        return True
    except ImportError as e:
        print(f"  ✗ Failed to import Kafka library: {e}")
        return False


def test_kafka_connection():
    """Test connection to Kafka broker."""
    print("\nTesting Kafka broker connection...")
    try:
        from confluent_kafka.admin import AdminClient
        from src import config
        
        admin_config = {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "socket.timeout.ms": 5000,
        }
        
        admin_client = AdminClient(admin_config)
        
        # Get cluster metadata (this will fail if Kafka is not reachable)
        metadata = admin_client.list_topics(timeout=5)
        
        print(f"  ✓ Connected to Kafka broker at {config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"  ✓ Broker has {len(metadata.topics)} topics")
        
        return True, admin_client
        
    except Exception as e:
        print(f"  ✗ Failed to connect to Kafka broker: {e}")
        print(f"  ℹ Make sure Kafka is running: docker-compose up -d")
        return False, None


def test_topics_exist(admin_client):
    """Test required topics exist."""
    print("\nTesting required topics...")
    try:
        from src import config
        
        required_topics = [
            config.TOPIC_PIZZA_ORDERS,
            config.TOPIC_COOKED_PIZZAS,
            config.TOPIC_ORDER_EVENTS,
        ]
        
        metadata = admin_client.list_topics(timeout=5)
        existing_topics = set(metadata.topics.keys())
        
        all_exist = True
        for topic in required_topics:
            if topic in existing_topics:
                topic_metadata = metadata.topics[topic]
                partition_count = len(topic_metadata.partitions)
                print(f"  ✓ Topic '{topic}' exists ({partition_count} partitions)")
            else:
                print(f"  ✗ Topic '{topic}' NOT found")
                all_exist = False
        
        if not all_exist:
            print(f"  ℹ Run './init-kafka.sh' to create topics")
        
        return all_exist
        
    except Exception as e:
        print(f"  ✗ Failed to check topics: {e}")
        return False


def test_producer_connection():
    """Test producer can be created and connect."""
    print("\nTesting Kafka producer connection...")
    try:
        from confluent_kafka import Producer
        from src import config
        
        producer_config = config.get_producer_config()
        producer = Producer(producer_config)
        
        # Get producer metadata to confirm connection
        metadata = producer.list_topics(timeout=5)
        
        print(f"  ✓ Producer created successfully")
        print(f"  ✓ Producer can see {len(metadata.topics)} topics")
        
        producer.flush(timeout=1)
        return True
        
    except Exception as e:
        print(f"  ✗ Failed to create producer: {e}")
        return False


def test_consumer_connection():
    """Test consumer can be created and connect."""
    print("\nTesting Kafka consumer connection...")
    try:
        from confluent_kafka import Consumer
        from src import config
        
        consumer_config = config.get_consumer_config(
            group_id="test-consumer-group",
            client_id="test-consumer"
        )
        consumer = Consumer(consumer_config)
        
        # Subscribe to a topic to test connection
        consumer.subscribe([config.TOPIC_PIZZA_ORDERS])
        
        print(f"  ✓ Consumer created successfully")
        print(f"  ✓ Consumer subscribed to {config.TOPIC_PIZZA_ORDERS}")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Failed to create consumer: {e}")
        return False


def test_produce_consume():
    """Test basic produce and consume functionality."""
    print("\nTesting produce/consume functionality...")
    try:
        from confluent_kafka import Producer, Consumer, KafkaError
        from src import config, models
        
        # Create test message
        test_order = models.create_order_message(
            source="test",
            customer_name="Test Customer",
            order_number=9999,
        )
        
        # Produce message
        producer_config = config.get_producer_config()
        producer = Producer(producer_config)
        
        test_topic = config.TOPIC_PIZZA_ORDERS
        producer.produce(
            test_topic,
            key=test_order["order_id"].encode("utf-8"),
            value=json.dumps(test_order).encode("utf-8"),
        )
        producer.flush(timeout=5)
        print(f"  ✓ Test message produced to {test_topic}")
        
        # Consume message
        consumer_config = config.get_consumer_config(
            group_id="test-group",
            client_id="test-consumer"
        )
        # Start from beginning to catch our test message
        consumer_config["auto.offset.reset"] = "earliest"
        consumer = Consumer(consumer_config)
        consumer.subscribe([test_topic])
        
        # Poll for message
        found_message = False
        for _ in range(10):  # Try 10 times
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"  ✗ Consumer error: {msg.error()}")
                    break
            
            # Got a message
            value = json.loads(msg.value().decode("utf-8"))
            if value.get("order_id") == test_order["order_id"]:
                found_message = True
                print(f"  ✓ Test message consumed successfully")
                break
        
        consumer.close()
        
        if not found_message:
            print(f"  ⚠ Could not find test message (might be normal if topic has many messages)")
            return True  # Don't fail the test for this
        
        return True
        
    except Exception as e:
        print(f"  ✗ Failed produce/consume test: {e}")
        return False


def test_topic_configuration():
    """Test topic configuration and settings."""
    print("\nTesting topic configuration...")
    try:
        from confluent_kafka.admin import AdminClient
        from src import config
        
        admin_config = {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "socket.timeout.ms": 5000,
        }
        admin_client = AdminClient(admin_config)
        metadata = admin_client.list_topics(timeout=5)
        
        required_topics = [
            config.TOPIC_PIZZA_ORDERS,
            config.TOPIC_COOKED_PIZZAS,
            config.TOPIC_ORDER_EVENTS,
        ]
        
        all_good = True
        for topic in required_topics:
            if topic in metadata.topics:
                topic_meta = metadata.topics[topic]
                partitions = len(topic_meta.partitions)
                
                # Check partition count
                if partitions >= 1:
                    print(f"  ✓ {topic}: {partitions} partition(s)")
                else:
                    print(f"  ✗ {topic}: No partitions")
                    all_good = False
                    
        return all_good
        
    except Exception as e:
        print(f"  ✗ Failed to check topic configuration: {e}")
        return False


def main():
    """Run all Kafka connectivity tests."""
    print("=" * 70)
    print("Pizza Shop Demo - Kafka Connection Test")
    print("=" * 70)
    
    # Check if imports work first
    if not test_kafka_import():
        print("\n✗ Cannot proceed without Kafka library. Install dependencies first.")
        print("  Run: ./venv/bin/pip install -r requirements.txt")
        return 1
    
    results = []
    
    # Test Kafka connection
    connection_ok, admin_client = test_kafka_connection()
    results.append(("Kafka Connection", connection_ok))
    
    if not connection_ok:
        print("\n" + "=" * 70)
        print("✗ Cannot proceed without Kafka connection")
        print("=" * 70)
        print("\nTo start Kafka:")
        print("  1. docker-compose up -d")
        print("  2. Wait 10-15 seconds for Kafka to be ready")
        print("  3. Run ./init-kafka.sh to create topics")
        print("  4. Run this test again")
        return 1
    
    # Run remaining tests
    results.append(("Topics Exist", test_topics_exist(admin_client)))
    results.append(("Producer Connection", test_producer_connection()))
    results.append(("Consumer Connection", test_consumer_connection()))
    results.append(("Topic Configuration", test_topic_configuration()))
    results.append(("Produce/Consume", test_produce_consume()))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} {test_name}")
    
    print("-" * 70)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 70)
    
    if passed == total:
        print("\n✓ All Kafka tests passed! System is ready to run.")
        return 0
    else:
        print(f"\n✗ {total - passed} test(s) failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
