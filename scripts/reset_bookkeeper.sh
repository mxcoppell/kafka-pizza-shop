#!/bin/bash

# Reset Bookkeeper Stats Script
# This script clears all events from the order-events topic, effectively resetting
# the bookkeeper's statistics when it restarts.

set -e

echo "=================================="
echo "Resetting Bookkeeper Statistics"
echo "=================================="
echo ""

# Check if Kafka container is running
if ! docker ps | grep -q pizza-shop-kafka; then
    echo "❌ Error: Kafka container 'pizza-shop-kafka' is not running"
    echo "   Start Kafka first: docker-compose up -d"
    exit 1
fi

echo "✓ Kafka container is running"
echo ""

# Delete the order-events topic
echo "📋 Deleting order-events topic..."
docker exec pizza-shop-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic order-events 2>/dev/null || true

echo "✓ Topic deleted"
echo ""

# Wait a moment for deletion to complete
sleep 2

# Recreate the order-events topic
echo "📋 Recreating order-events topic..."
docker exec pizza-shop-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic order-events \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=3600000

echo "✓ Topic recreated"
echo ""

# Verify topic was created
echo "📋 Verifying topic..."
docker exec pizza-shop-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic order-events

echo ""
echo "=================================="
echo "✅ Bookkeeper stats reset complete!"
echo "=================================="
echo ""
echo "Next steps:"
echo "  1. Start the bookkeeper: python -m src.consumers.bookkeeper"
echo "  2. The dashboard will start fresh with zero statistics"
echo ""
