#!/bin/bash
# init-kafka.sh
# Initializes Kafka topics for the Pizza Shop demo system
# This script waits for Kafka to be ready, then creates the required topics

set -e

echo "=========================================="
echo "Pizza Shop Kafka Initialization"
echo "=========================================="
echo ""

# Kafka broker connection string
KAFKA_BROKER="kafka:9092"

# Maximum wait time for Kafka to be ready (seconds)
MAX_WAIT=60
WAIT_INTERVAL=2
elapsed=0

echo "⏳ Waiting for Kafka broker to be ready at $KAFKA_BROKER..."
echo ""

# Wait for Kafka to be ready
while [ $elapsed -lt $MAX_WAIT ]; do
    if kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER &> /dev/null; then
        echo "✅ Kafka broker is ready!"
        echo ""
        break
    fi
    
    echo "   Waiting... (${elapsed}s / ${MAX_WAIT}s)"
    sleep $WAIT_INTERVAL
    elapsed=$((elapsed + WAIT_INTERVAL))
done

# Check if we timed out
if [ $elapsed -ge $MAX_WAIT ]; then
    echo "❌ ERROR: Kafka broker did not become ready within ${MAX_WAIT} seconds"
    exit 1
fi

echo "=========================================="
echo "Creating Kafka Topics"
echo "=========================================="
echo ""

# Function to create a topic with error handling
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3
    local cleanup_policy=$4
    local retention_ms=$5
    
    echo "📝 Creating topic: $topic_name"
    echo "   - Partitions: $partitions"
    echo "   - Replication Factor: $replication_factor"
    echo "   - Cleanup Policy: $cleanup_policy"
    
    if [ -n "$retention_ms" ]; then
        echo "   - Retention: ${retention_ms}ms ($(($retention_ms / 1000 / 60 / 60 / 24)) days)"
    fi
    
    # Build the create command
    local cmd="kafka-topics --create \
        --bootstrap-server $KAFKA_BROKER \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication_factor \
        --config cleanup.policy=$cleanup_policy"
    
    # Add retention if specified
    if [ -n "$retention_ms" ]; then
        cmd="$cmd --config retention.ms=$retention_ms"
    fi
    
    # Execute the command
    if eval $cmd 2>&1; then
        echo "   ✅ Topic '$topic_name' created successfully"
    else
        # Check if topic already exists
        if kafka-topics --list --bootstrap-server $KAFKA_BROKER | grep -q "^${topic_name}$"; then
            echo "   ℹ️  Topic '$topic_name' already exists, skipping"
        else
            echo "   ❌ ERROR: Failed to create topic '$topic_name'"
            return 1
        fi
    fi
    echo ""
}

# Create pizza-orders topic
# - 3 partitions for parallel processing by 3 pizza makers
# - Replication factor 1 (single broker demo environment)
# - Delete cleanup policy (standard message deletion after retention)
# - 7 days retention
create_topic "pizza-orders" 3 1 "delete" "604800000"

# Create cooked-pizzas topic
# - 3 partitions for parallel processing by 3 delivery workers
# - Replication factor 1 (single broker demo environment)
# - Delete cleanup policy
# - 7 days retention
create_topic "cooked-pizzas" 3 1 "delete" "604800000"

# Create order-events topic
# - 1 partition to ensure total ordering of events for bookkeeping
# - Replication factor 1 (single broker demo environment)
# - Compact cleanup policy to keep latest state per order
# - 30 days retention for audit trail
create_topic "order-events" 1 1 "compact" "2592000000"

echo "=========================================="
echo "Verifying Topics"
echo "=========================================="
echo ""

# List all topics to verify creation
echo "📋 All Kafka topics:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo ""
echo "=========================================="
echo "Topic Details"
echo "=========================================="
echo ""

# Describe each topic to show configuration
for topic in "pizza-orders" "cooked-pizzas" "order-events"; do
    echo "📊 Details for topic: $topic"
    kafka-topics --describe --bootstrap-server $KAFKA_BROKER --topic $topic
    echo ""
done

echo "=========================================="
echo "✅ Kafka initialization completed successfully!"
echo "=========================================="
echo ""
echo "Topics ready for Pizza Shop demo:"
echo "  • pizza-orders (3 partitions)"
echo "  • cooked-pizzas (3 partitions)"
echo "  • order-events (1 partition)"
echo ""
echo "You can now start the Python producers and consumers."
echo ""
