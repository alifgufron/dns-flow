# Kafka Setup Guide

## FreeBSD (pkg) — Kafka 4.x (KRaft-only)

> **Kafka 4.x removes ZooKeeper completely.** KRaft mode is the only mode.
> The FreeBSD pkg installs config files at `/usr/local/etc/kafka/` (no `kraft/` subdirectory).
> The `kafka-storage.sh format` command requires `--standalone` for single-node setups.

```bash
# Install Kafka (Java OpenJDK is a dependency, installed automatically)
pkg install -y kafka

# Verify Java
java -version

# Enable at boot (use the default pkg config path — no kraft/ subdirectory)
sysrc kafka_enable="YES"
sysrc kafka_config="/usr/local/etc/kafka/server.properties"

# ------------------------------------------------------------------
# Copy sample config files provided by pkg
# ------------------------------------------------------------------
cp /usr/local/etc/kafka/server.properties.sample \
   /usr/local/etc/kafka/server.properties

cp /usr/local/etc/kafka/log4j2.yaml.sample \
   /usr/local/etc/kafka/log4j2.yaml

# Optional: edit server.properties if you need to change advertised.listeners
# By default it binds to localhost:9092 which is fine for single-node
# vi /usr/local/etc/kafka/server.properties

# ------------------------------------------------------------------
# Generate Cluster ID and format storage (KRaft single-node)
# NOTE: Kafka 4.x requires --standalone flag for single-node format
# ------------------------------------------------------------------
KAFKA_HOME=/usr/local/share/java/kafka
UUID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $UUID"

su -m kafka -c "$KAFKA_HOME/bin/kafka-storage.sh format \
  --standalone \
  -t $UUID \
  -c /usr/local/etc/kafka/server.properties"

# ------------------------------------------------------------------
# Start Kafka
# ------------------------------------------------------------------
service kafka start
service kafka status
```

Default ports after start:
- `9092` — Broker listener (producer/consumer)
- `9093` — Controller listener (KRaft internal)

Data directory (pkg default): `/var/db/kafka`

## Verify Cluster

```bash
KAFKA_HOME=/usr/local/share/java/kafka

# Check KRaft metadata quorum
$KAFKA_HOME/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller localhost:9093 \
  describe --status

# Expected output:
# ClusterId:    <UUID>
# LeaderId:     1
# LeaderEpoch:  1

# List topics
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create dns.raw topic manually (optional, auto-created by dns-flow)
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --topic dns.raw \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

## Linux (manual tar)

```bash
# Download Kafka (adjust version URL as needed)
KAFKA_VERSION=4.0.0
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
tar xzf kafka_2.13-${KAFKA_VERSION}.tgz
sudo mv kafka_2.13-${KAFKA_VERSION} /opt/kafka

# Create kafka user and data directory
sudo useradd -r -s /bin/false kafka
sudo mkdir -p /var/lib/kafka /etc/kafka /var/log/kafka
sudo chown kafka:kafka /var/lib/kafka /var/log/kafka

# Copy config
sudo cp /opt/kafka/config/kraft/server.properties /etc/kafka/server.properties

# Generate Cluster ID and format (single-node)
KAFKA_HOME=/opt/kafka
UUID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
sudo -u kafka $KAFKA_HOME/bin/kafka-storage.sh format \
  --standalone \
  -t $UUID \
  -c /etc/kafka/server.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon /etc/kafka/server.properties
```

## KRaft `server.properties` — Key Settings

The FreeBSD pkg sample (`server.properties.sample`) is pre-configured for single-node KRaft.
Key settings to verify:

```properties
# Node identity
node.id=1

# KRaft roles (combined broker+controller on single node)
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER

# Listeners
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

# Data directory (FreeBSD pkg default)
log.dirs=/var/db/kafka

# Auto-create topics (dns-flow uses admin API to create dns.raw)
auto.create.topics.enable=true

# Single-node replication
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

## Systemd Service (Linux)

`/etc/systemd/system/kafka.service`:

```ini
[Unit]
Description=Apache Kafka (KRaft)
After=network.target

[Service]
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
```

## rc.d Service (FreeBSD)

The `/usr/local/etc/rc.d/kafka` script is installed automatically with `pkg install kafka`.
Configure in `/etc/rc.conf`:

```
kafka_enable="YES"
kafka_config="/usr/local/etc/kafka/server.properties"
```

## Topic & Partition

### Auto-Creation

At startup, if topic `dns.raw` does not exist, dns-flow creates it automatically via the Kafka admin API (1 partition, 1 replication). Suitable for development.

### Create topic manually (production)

For production, create the topic **before** running dns-flow with the desired partition count:

```bash
KAFKA_HOME=/usr/local/share/java/kafka

# 6 partitions, replication-factor 1 (single node)
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --topic dns.raw \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### Partition count guideline

| Brokers | Partitions | Reason |
|---------|-----------|--------|
| 1 (dev) | 1 | Auto-create is sufficient |
| 3 | 6 | 2x brokers, good balancing |
| 6 | 12-18 | 2-3x brokers |
| N | N*2 to N*3 | Rule of thumb |

### Choosing partition count

1. **Hash by qname** — `dns-flow` uses `kafka.Hash{}` balancer; events with the same qname always go to the same partition. Ordering guaranteed per-qname.
2. **Too many partitions** (>100) — increases consumer group rebalance time and metadata overhead
3. **Too few partitions** (<3) — throughput bottleneck, limited consumer parallelism

### Verify topic

```bash
KAFKA_HOME=/usr/local/share/java/kafka
$KAFKA_HOME/bin/kafka-topics.sh --describe --topic dns.raw --bootstrap-server localhost:9092
```

### Replay from beginning

```bash
KAFKA_HOME=/usr/local/share/java/kafka
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group dns-flow --topic dns.raw --reset-offsets --to-earliest --execute
```
