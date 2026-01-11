# 🍕 Kafka Pizza Shop - Interactive Learning Demo

A hands-on demonstration system that teaches Apache Kafka concepts through a real-world pizza shop order fulfillment simulation. Watch as orders flow through topics, consumers process them in parallel, and a complete event-driven architecture comes to life!

![Kafka Pizza Shop Architecture](kafka-pizza-shop.png)

*A visual representation of our event-driven pizza shop: orders flow from web and frontdesk terminals through Kafka's distributed system, where 3 pizza makers cook in parallel, 3 delivery workers deliver pizzas, and a bookkeeper tracks everything in real-time.*

## 📖 Overview

This project simulates a pizza shop's complete order workflow using Apache Kafka for event streaming. It demonstrates how distributed systems handle real-time data processing through a fun, easy-to-understand scenario.

**Perfect for:** Kafka beginners, students learning distributed systems, and developers who want practical experience with event-driven architectures.

## 🎯 What You'll Learn

This demo showcases essential Kafka concepts in action:

- **📬 Topics & Partitions**: Three topics with strategic partition counts for parallel processing
- **🏭 Producers**: Multiple independent order sources publishing to the same topics
- **👥 Consumer Groups**: Parallel processing with automatic load distribution
- **📊 Event Streaming**: Complete order lifecycle tracking through event logs
- **🔄 Message Flow**: How data moves through multi-stage pipelines
- **⚖️ Load Balancing**: Watch Kafka distribute work across multiple consumers
- **📈 Parallel Processing**: See 3 pizza makers and 3 delivery workers handling orders simultaneously

**Real-world patterns demonstrated:**

- Event-driven architecture
- Microservices communication
- Data enrichment in streaming pipelines
- Real-time monitoring and analytics

## 🏗️ System Architecture

The demo simulates a complete pizza shop with:

### Components

- **2 Order Producers**: Web interface and front desk (simulate different order sources)
- **3 Pizza Makers**: Cook pizzas in parallel (consumer group: `pizza-makers`)
- **3 Delivery Workers**: Deliver pizzas in parallel (consumer group: `delivery-workers`)
- **1 Bookkeeper**: Tracks all events and displays real-time dashboard

### 4-Terminal Simplified Approach

To make the demo easier to run, we use **component runners** that start multiple instances in a single terminal:

| Terminal | Component | What It Does |
|----------|-----------|--------------|
| Terminal 1 | **Producers Runner** | Runs both web and frontdesk producers concurrently (200 orders total) |
| Terminal 2 | **Pizza Makers Runner** | Starts 3 pizza maker instances in parallel threads |
| Terminal 3 | **Delivery Workers Runner** | Starts 3 delivery worker instances in parallel threads |
| Terminal 4 | **Bookkeeper Dashboard** | Real-time monitoring and analytics |

**Benefits of this approach:**

- Reduces complexity from 8 terminals to 4
- Easier to manage and monitor
- All logs from each component type grouped together
- Graceful shutdown with a single Ctrl+C per terminal
- Uses threading for efficient parallel execution

### Kafka Topics

- **`pizza-orders`** (3 partitions): New orders from web/frontdesk
- **`cooked-pizzas`** (3 partitions): Pizzas ready for delivery
- **`order-events`** (1 partition): Complete audit trail of all order events

### Data Flow

```
Web/Frontdesk → pizza-orders → Pizza Makers → cooked-pizzas → Delivery Workers
                     ↓                ↓                              ↓
                order-events ← cooking events ← delivery events ← Bookkeeper
```

### Architecture Details

**Message Schemas:**

All messages use structured JSON format with clear schemas:

```json
{
  "order_id": "uuid",
  "timestamp": "ISO-8601",
  "customer_name": "string",
  "pizzas": [{"pizza_type": "string", "quantity": int}],
  "total_pizzas": int
}
```

**Consumer Groups:**

| Consumer Group | Members | Topic | Partitions | Strategy |
|----------------|---------|-------|------------|----------|
| `pizza-makers` | 3 pizza makers | `pizza-orders` | 3 | Each worker gets 1 partition |
| `delivery-workers` | 3 delivery workers | `cooked-pizzas` | 3 | Each worker gets 1 partition |
| `bookkeeper` | 1 bookkeeper | `order-events` | 1 | Single consumer reads all events |

**Key Design Decisions:**

- **3 Partitions**: Matches worker count for optimal parallelism and load distribution
- **Single partition for events**: Ensures strict chronological ordering for audit trail
- **Data enrichment**: Cooked pizza messages include original order data
- **Event-driven**: Complete separation between order processing and monitoring

## ✅ Prerequisites

Before you begin, ensure you have:

- **Docker Desktop** (with at least 4GB RAM allocated)
- **Python 3.9+** (Python 3.11+ recommended)
- **macOS** or Linux (instructions tailored for macOS)
- **10-15 minutes** for setup and first run

Verify your installations:

```bash
docker --version
python3 --version
```

## 🚀 Installation & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/kafka-pizza-shop.git
cd kafka-pizza-shop
```

*Note: Replace `YOUR_USERNAME` with your actual GitHub username after publishing the repository.*

### Step 2: Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:

- Apache Kafka 7.7.0 (Confluent Platform) in KRaft mode (no Zookeeper required)
- Automatically creates the three required topics
- Sets up health checks and proper networking

Wait ~30 seconds for Kafka to initialize. Verify it's running:

```bash
docker-compose ps
```

You should see `pizza-shop-kafka` service as "healthy".

**About KRaft Mode:**

- KRaft is Kafka's built-in consensus protocol that replaced Zookeeper
- Simpler architecture with fewer moving parts
- Better performance and lower latency
- Production-ready since Kafka 3.3+

### Step 3: Set Up Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

This installs:

- `confluent-kafka==2.3.0` - Kafka Python client library
- `python-dotenv==1.0.0` - Environment variable management
- `typing-extensions==4.9.0` - Type hint support

**Troubleshooting macOS:** If `confluent-kafka` installation fails:

```bash
brew install librdkafka
pip install confluent-kafka
```

### Step 4: Create Environment Configuration

```bash
cp .env.example .env
```

The default values work for local development. Key settings:

```bash
KAFKA_BOOTSTRAP_SERVERS_HOST=localhost:9093  # For Python on host
TOTAL_ORDERS=200              # Number of orders in demo
LOG_LEVEL=INFO                # Logging verbosity
```

**Important:** Use `localhost:9093` when running Python scripts on your host machine, and `kafka:9092` when running inside Docker containers.

### Step 5: Verify Setup

Verify Kafka topics were created:

```bash
docker exec pizza-shop-kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

You should see:

- `pizza-orders`
- `cooked-pizzas`
- `order-events`

## 🚀 Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
# Wait ~30 seconds for Kafka to become healthy
docker-compose ps
```

### 2. Set Up Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 3. Run the Demo (4 terminals needed)

**Important:** Start consumers first so they're ready to process orders!

**Terminal 1: Pizza Makers (3 makers working in parallel)**

```bash
source venv/bin/activate
python -m src.consumers.run_pizza_makers
```

**Terminal 2: Delivery Workers (3 workers delivering in parallel)**

```bash
source venv/bin/activate
python -m src.consumers.run_delivery_workers
```

**Terminal 3: Bookkeeper Dashboard (Real-time analytics)**

```bash
source venv/bin/activate
python -m src.consumers.bookkeeper
```

**Terminal 4: Producers (Start last - generates all 200 orders)**

```bash
source venv/bin/activate
python -m src.producers.simulate_orders
```

### 4. Watch the Demo

- **Terminal 1**: Observe pizza makers cooking orders in parallel (all 3 makers' logs combined)
- **Terminal 2**: Watch delivery workers picking up and delivering pizzas (all 3 workers' logs combined)
- **Terminal 3**: Monitor the bookkeeper dashboard for real-time statistics
- **Terminal 4**: View producer statistics when all orders are generated
- Demo completes in **~6 seconds** for 200 orders (3x faster than original)

### 5. Stop the Demo

Press Ctrl+C in each terminal to stop the components gracefully, then:

```bash
docker-compose down
```

### Advanced: Running Individual Components

If you want to run individual pizza makers or delivery workers in separate terminals for debugging or learning purposes:

**Individual Pizza Maker:**

```bash
source venv/bin/activate
python -m src.consumers.pizza_maker maker-1
```

**Individual Delivery Worker:**

```bash
source venv/bin/activate
python -m src.consumers.delivery_worker driver-1 driver
```

This allows you to observe individual consumer behavior and Kafka's partition assignment in detail.

## 🎮 Expected Output

**Bookkeeper Dashboard Example:**

```
╔═══════════════════════════════════════════════════════════════╗
║              🍕 PIZZA SHOP LIVE DASHBOARD 🍕                  ║
╚═══════════════════════════════════════════════════════════════╝

📊 Order Status:
  Orders Created:    200
  Orders Cooking:      8
  Orders Cooked:     175
  Orders Delivering:   5
  Orders Delivered:  167

⏱️  Performance Metrics:
  Average Cooking Time:   0.82 seconds
  Average Delivery Time:  2.14 seconds
  Total Throughput:       45.5 orders/min

🕐 Updated: 2026-01-11 02:15:34
```

**Pizza Maker Log Example:**

```
[INFO] Pizza Maker maker-2 cooked order #42 (3 pizzas) in 2.4s
[INFO] Pizza Maker maker-1 cooked order #43 (1 pizza) in 0.8s
[INFO] Pizza Maker maker-3 cooked order #44 (2 pizzas) in 1.6s
```

## ⚙️ Configuration

### Environment Variables

Edit [`.env`](.env) to customize:

```bash
# Kafka Connection (for Python on host machine)
KAFKA_BOOTSTRAP_SERVERS_HOST=localhost:9093

# Demo Settings
TOTAL_ORDERS=200              # Number of orders to generate
WEB_ORDERS=120                # Orders from web (60%)
FRONTDESK_ORDERS=80           # Orders from frontdesk (40%)

# Timing (3x faster for quicker demo)
COOKING_TIME_MULTIPLIER=0.33  # Adjust simulation speed (0.33 = 3x faster)
DELIVERY_TIME_MIN=0.33        # Min delivery time (seconds)
DELIVERY_TIME_MAX=1.0         # Max delivery time (seconds)

# Logging
LOG_LEVEL=INFO                # DEBUG, INFO, WARNING, ERROR
```

### Pizza Types and Cooking Times

10 pizza types with varying complexity (0.2-2.0 seconds cooking time):

| Pizza Type | Cooking Time | Complexity |
|------------|--------------|------------|
| Margherita | 0.2s | Simple |
| Pepperoni | 0.4s | Simple |
| Hawaiian | 0.6s | Simple |
| Vegetarian | 0.8s | Medium |
| BBQ Chicken | 1.0s | Medium |
| Meat Lovers | 1.2s | Medium |
| Four Cheese | 1.4s | Complex |
| Supreme | 1.6s | Complex |
| Seafood Special | 1.8s | Complex |
| Deluxe Everything | 2.0s | Complex |

See full configuration in [`src/config.py`](src/config.py)

### Connection Information

**For Python Clients on Host Machine (macOS):**

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
```

**For Python Clients in Docker Network:**

```python
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
```

## 🧪 Testing

### Environment Validation Test

Verify your Python environment and dependencies:

```bash
source venv/bin/activate
python tests/test_setup.py
```

**Expected Result:** All 7 tests should PASS

- Python version check
- Dependencies installed
- Config files present
- Source structure valid
- Module imports successful
- Configuration loading
- Data models validated

### Kafka Connectivity Test

Test connection to Kafka broker:

```bash
# Ensure Kafka is running first
docker-compose up -d

# Wait for Kafka to be healthy (~30 seconds)
docker-compose ps

# Run connectivity test
python tests/test_kafka_connection.py
```

**Expected Result:** All connectivity tests should PASS

- Kafka broker connection successful
- Topics exist and accessible
- Producer can connect and send messages
- Consumer can connect and receive messages

### End-to-End System Test

Run the complete demo workflow with 200 orders by following the [Quick Start](#-quick-start) section to start all components in separate terminals.

**Expected Results:**

- **Total Orders**: 200 (120 web, 80 frontdesk)
- **Processing Time**: ~17 seconds
- **Throughput**: 11.99 orders/second
- **Success Rate**: 100% (all orders processed)
- **Errors**: Zero

### Quick Validation Test (50 orders)

For a quick test, reduce order count:

```bash
# Edit .env file
TOTAL_ORDERS=50
WEB_ORDERS=30
FRONTDESK_ORDERS=20

# Run demo (expected time: ~5 seconds)
# Follow Quick Start instructions with reduced order count
```

### Common Kafka Debug Commands

**List all topics:**

```bash
docker exec pizza-shop-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

**Describe a specific topic (show partitions, replicas):**

```bash
docker exec pizza-shop-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic pizza-orders
```

**View messages in a topic (from beginning):**

```bash
docker exec pizza-shop-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pizza-orders \
  --from-beginning \
  --max-messages 10
```

**View messages in a topic (real-time):**

```bash
docker exec pizza-shop-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pizza-orders
```

**Check consumer group status and lag:**

```bash
docker exec pizza-shop-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group pizza-makers
```

**List all consumer groups:**

```bash
docker exec pizza-shop-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

**Count messages in a topic:**

```bash
docker exec pizza-shop-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic pizza-orders \
  --time -1
```

**Delete a topic (caution!):**

```bash
docker exec pizza-shop-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic pizza-orders
```

**View Kafka broker logs:**

```bash
docker logs pizza-shop-kafka
```

**Check Kafka broker configuration:**

```bash
docker exec pizza-shop-kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type brokers \
  --entity-name 1
```

### Monitoring Kafka Topics

**View orders being created:**

```bash
docker exec pizza-shop-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic pizza-orders \
  --from-beginning
```

**View pizzas being cooked:**

```bash
docker exec pizza-shop-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cooked-pizzas \
  --from-beginning
```

**View all events:**

```bash
docker exec pizza-shop-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning
```

**Check consumer group status:**

```bash
# Pizza makers group
docker exec pizza-shop-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group pizza-makers

# Delivery workers group
docker exec pizza-shop-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group delivery-workers
```

## 📊 Performance Metrics

### Validated Performance (200-Order Test)

The system has been optimized for a fast demo experience:

| Metric | Result | Status |
|--------|--------|--------|
| Total Orders | 200 | ✅ |
| Orders from Web | 120 | ✅ |
| Orders from Frontdesk | 80 | ✅ |
| Processing Time | **~6 seconds** | ✅ |
| Throughput | **~33 orders/sec** | ✅ |
| Success Rate | 100% | ✅ |
| Failed Orders | 0 | ✅ |
| System Errors | 0 | ✅ |

### Component Performance

**Pizza Makers** (3 instances):

- Parallel processing across 3 partitions
- Load balanced automatically by Kafka
- Average cooking time per order: 0.27-0.66 seconds (3x faster, varies by pizza complexity)

**Delivery Workers** (3 instances):

- Parallel delivery across 3 partitions
- Load balanced automatically by Kafka
- Delivery time range: 0.33-1.0 seconds per order (3x faster)

**Bookkeeper**:

- Real-time event processing
- Dashboard updates every second
- Tracks complete order lifecycle

## 🔧 Troubleshooting

### Kafka Won't Start

**Problem**: Docker container fails to start or stays unhealthy

**Solutions**:

```bash
# Check Docker is running
docker ps

# Check logs for errors
docker-compose logs kafka

# Reset and restart (removes all data!)
docker-compose down -v
docker-compose up -d
```

### Python Can't Connect to Kafka

**Problem**: `NoBrokersAvailable` or connection timeout errors

**Solutions**:

- ✅ Verify Kafka is healthy: `docker-compose ps`
- ✅ Check you're using `localhost:9093` (not `kafka:9092`) in `.env`
- ✅ Ensure virtual environment is activated: `source venv/bin/activate`
- ✅ Wait 30 seconds after `docker-compose up` for full initialization

### Topics Not Found

**Problem**: Consumers can't find topics

**Solutions**:

```bash
# Verify topics exist
docker exec pizza-shop-kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

# Check topic details
docker exec pizza-shop-kafka kafka-topics \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic pizza-orders
```

If topics are missing, check [`scripts/init-kafka.sh`](scripts/init-kafka.sh) ran successfully:

```bash
docker-compose logs kafka-init
```

### Consumer Group Not Working

**Problem**: All consumers receive the same messages

**Solutions**:

- Ensure consumers use same `group.id` (e.g., `pizza-makers`)
- Check consumer group status:

```bash
docker exec pizza-shop-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group pizza-makers
```

### Orders Processing Slowly

**Problem**: Demo takes too long

**Solutions**:

- Reduce `TOTAL_ORDERS` in `.env` (try 50 for quick test)
- Adjust `COOKING_TIME_MULTIPLIER=0.5` for faster cooking
- Check all 3 pizza makers and 3 delivery workers are running

### librdkafka Installation Fails (macOS)

**Problem**: `confluent-kafka` won't install

**Solution**:

```bash
# Install librdkafka first
brew install librdkafka

# Then install Python package
pip install confluent-kafka
```

### Need to Reset Demo

**Solution**: Clean restart with fresh data

```bash
# Stop everything
docker-compose down -v

# Remove Python cache
find . -type d -name __pycache__ -exec rm -rf {} +

# Start fresh
docker-compose up -d
```

### Reset Bookkeeper Statistics

**Problem**: Want to reset bookkeeper statistics without clearing all topics

**Solution**: Use the reset script

```bash
# Stop bookkeeper if running (Ctrl+C in Terminal 3)

# Reset bookkeeper stats (clears order-events topic)
./scripts/reset_bookkeeper.sh

# Restart bookkeeper - it will start with fresh statistics
source venv/bin/activate
python -m src.consumers.bookkeeper
```

This script:

- Deletes and recreates the `order-events` topic
- Preserves `pizza-orders` and `cooked-pizzas` topics
- Allows bookkeeper to start with zero statistics
- Does not affect running pizza makers or delivery workers

### Ports Already in Use

**Problem**: Kafka can't bind to port 9092 or 9093

**Solution**:

```bash
# Find processes using the ports
lsof -i :9092
lsof -i :9093

# Kill the processes or change ports in docker-compose.yml
```

## 📁 Project Structure

```
learn.kafka/
├── README.md                      # Complete documentation
├── docker-compose.yml             # Kafka infrastructure
├── requirements.txt               # Python dependencies
├── .env.example                   # Configuration template
├── .gitignore                     # Git ignore rules
│
├── scripts/
│   └── init-kafka.sh             # Topic creation script
│
├── tests/
│   ├── test_setup.py             # Environment validation
│   └── test_kafka_connection.py  # Kafka connectivity test
│
└── src/
    ├── config.py                  # Configuration and constants
    ├── models.py                  # Message schemas and data models
    │
    ├── producers/
    │   ├── order_producer.py      # Base producer class
    │   ├── web_producer.py        # Web order producer
    │   ├── frontdesk_producer.py  # Front desk producer
    │   └── simulate_orders.py     # Order simulation runner (Terminal 1)
    │
    ├── consumers/
    │   ├── pizza_maker.py         # Pizza maker consumer (single instance)
    │   ├── delivery_worker.py     # Delivery worker consumer (single instance)
    │   ├── bookkeeper.py          # Analytics and monitoring (Terminal 4)
    │   ├── run_pizza_makers.py    # Pizza makers runner - 3 instances (Terminal 2)
    │   └── run_delivery_workers.py # Delivery workers runner - 3 instances (Terminal 3)
    │
    └── utils/
        └── __init__.py            # Logging and helper utilities
```

### Key Files

- **[`src/producers/simulate_orders.py`](src/producers/simulate_orders.py)**: Runs both web and frontdesk producers (Terminal 1)
- **[`src/consumers/run_pizza_makers.py`](src/consumers/run_pizza_makers.py)**: Starts 3 pizza maker instances in parallel (Terminal 2)
- **[`src/consumers/run_delivery_workers.py`](src/consumers/run_delivery_workers.py)**: Starts 3 delivery worker instances in parallel (Terminal 3)
- **[`src/consumers/bookkeeper.py`](src/consumers/bookkeeper.py)**: Real-time monitoring dashboard (Terminal 4)
- **[`src/consumers/pizza_maker.py`](src/consumers/pizza_maker.py)**: Single pizza maker consumer class
- **[`src/consumers/delivery_worker.py`](src/consumers/delivery_worker.py)**: Single delivery worker consumer class
- **[`docker-compose.yml`](docker-compose.yml)**: Kafka infrastructure definition
- **[`src/config.py`](src/config.py)**: All configuration values
- **[`scripts/init-kafka.sh`](scripts/init-kafka.sh)**: Topic creation script

## 🎓 Kafka Concepts Demonstrated

### Topics and Partitions

- **Multiple partitions** (`pizza-orders`: 3, `cooked-pizzas`: 3) enable parallel processing
- **Single partition** (`order-events`: 1) ensures ordered event stream
- Learn how partition count affects scalability and parallelism

### Producer Patterns

- **Multiple producers** to same topic (web + frontdesk → `pizza-orders`)
- **Key-based partitioning** for message routing
- **Event publishing** to multiple topics simultaneously
- **Message serialization** using JSON

### Consumer Groups

- **Load distribution**: 3 pizza makers share `pizza-orders` partitions
- **Automatic rebalancing**: Kafka assigns partitions to group members
- **Independent groups**: Bookkeeper reads same topics without affecting pizza makers
- **Parallel processing**: Multiple consumers process different partitions simultaneously

### Event Streaming

- **Event sourcing**: All state changes published as events
- **Data enrichment**: Cooked pizza messages include original order data
- **Audit trail**: Complete order lifecycle in `order-events` topic
- **Real-time analytics**: Bookkeeper computes metrics from event stream

### Message Flow Example

Complete lifecycle of Order #42:

1. Web interface publishes order to `pizza-orders` topic
2. Web interface publishes `order_created` event to `order-events`
3. Pizza Maker 2 consumes from `pizza-orders`
4. Pizza Maker 2 publishes `cooking_started` event
5. Pizza Maker 2 simulates cooking (3.8 seconds)
6. Pizza Maker 2 publishes cooked pizza to `cooked-pizzas`
7. Pizza Maker 2 publishes `cooking_completed` event
8. Delivery Worker 1 consumes from `cooked-pizzas`
9. Delivery Worker 1 publishes `delivery_started` event
10. Delivery Worker 1 simulates delivery (2.3 seconds)
11. Delivery Worker 1 publishes `delivery_completed` event
12. Bookkeeper tracks all events and updates statistics

## 📚 Learning Resources

### Kafka Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) - Official docs
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html) - Client library used in this project
- [KRaft Mode Overview](https://kafka.apache.org/documentation/#kraft) - Understanding Kafka's new consensus protocol
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/) - Comprehensive book

### Understanding This Demo

1. **Start here**: Run the quick start to see it in action
2. **Explore code**: Check [`src/consumers/pizza_maker.py`](src/consumers/pizza_maker.py) for consumer patterns
3. **Study producers**: Review [`src/producers/order_producer.py`](src/producers/order_producer.py) for producer patterns
4. **Experiment**: Modify pizza types in [`src/config.py`](src/config.py)
5. **Scale**: Try adding more consumers or changing partition counts

## 🤝 Contributing

This is a learning project! Feel free to:

- Report bugs or issues
- Suggest improvements
- Add new features (order cancellation, failed deliveries, etc.)
- Create alternative demos
- Improve documentation

## 🎉 Next Steps

Now that you understand the demo:

1. **Experiment**: Modify configuration values and see what happens
2. **Break things**: Stop a consumer mid-processing, restart it, observe Kafka's behavior
3. **Extend it**: Add new features like order cancellation or priority orders
4. **Build your own**: Use this as a template for your own Kafka projects

### Learning Exercises

**Exercise 1: Understanding the 4-Terminal Approach**

1. Start all 4 terminals in the Quick Start guide
2. Observe how runner scripts manage multiple instances
3. Note the combined logging from all instances
4. Press Ctrl+C in Terminal 2 and watch graceful shutdown
5. Restart Terminal 2 and observe Kafka's rebalancing

**Exercise 2: Consumer Scaling with Runners**

1. Modify [`src/consumers/run_pizza_makers.py`](src/consumers/run_pizza_makers.py) to start only 1 maker
2. Run the demo and observe partition assignment
3. Change back to 3 makers
4. Watch Kafka distribute partitions evenly
5. Monitor throughput improvement

**Exercise 3: Individual Component Debugging**

1. Use the Advanced instructions to run individual pizza makers in separate terminals
2. Start maker-1, maker-2, and maker-3 in 3 different terminals
3. Observe how each gets assigned to different partitions
4. Compare this to the 4-terminal runner approach
5. Understand the tradeoff between visibility and simplicity

**Exercise 4: Failure Recovery**

1. Start full demo (4 terminals)
2. Press Ctrl+C in Terminal 2 (kills all pizza makers)
3. Observe delivery workers waiting for cooked pizzas
4. Restart Terminal 2 (all makers restart)
5. Watch Kafka reassign partitions and resume processing
6. Verify no orders are lost

**Exercise 5: Performance Tuning**

1. Adjust `COOKING_TIME_MULTIPLIER` in `.env`
2. Modify number of makers in [`run_pizza_makers.py`](src/consumers/run_pizza_makers.py)
3. Change partition counts in topics (requires recreating topics)
4. Measure throughput changes
5. Document findings

## 📄 License

This project is open source and available for educational purposes.

---

**Ready to start?** Jump to [Quick Start](#-quick-start) and run your first Kafka demo in 5 minutes! 🚀

---

<div align="center">

**Made with ❤️ for Kafka learners everywhere**

[Report Bug](https://github.com/YOUR_USERNAME/kafka-pizza-shop/issues) · [Request Feature](https://github.com/YOUR_USERNAME/kafka-pizza-shop/issues)

*Note: Update YOUR_USERNAME with your GitHub username after publishing*

</div>
