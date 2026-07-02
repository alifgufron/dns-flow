# Kafka Setup Guide

## FreeBSD (pkg)

```bash
# Install
pkg install kafka

# Enable at boot
sysrc kafka_enable="YES"
sysrc kafka_config="/usr/local/etc/kafka/kraft/server.properties"

# Generate Cluster ID (KRaft)
KAFKA_HOME=/usr/local/share/java/kafka
$KAFKA_HOME/bin/kafka-storage.sh random-uuid
# output example: Z9-st5SYT1i6aFAOVpsWXQ

# Format storage directory with the UUID
$KAFKA_HOME/bin/kafka-storage.sh format \
  -t <UUID> \
  -c /usr/local/etc/kafka/kraft/server.properties

# Start Kafka
service kafka start
service kafka status
```

Default ports after start:
- `9092` — Broker listener (producer/consumer)
- `9093` — Controller listener (KRaft internal)

## Linux (manual tar)

```bash
# Download Kafka (adjust version URL as needed)
KAFKA_VERSION=3.9.2
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
tar xzf kafka_2.13-${KAFKA_VERSION}.tgz
sudo mv kafka_2.13-${KAFKA_VERSION} /opt/kafka

# Copy configuration
sudo mkdir -p /etc/kafka
sudo cp /opt/kafka/config/kraft/server.properties /etc/kafka/

# Create kafka user
sudo useradd -r -s /bin/false kafka
sudo mkdir -p /var/lib/kafka
sudo chown kafka:kafka /var/lib/kafka

# Generate Cluster ID and format
KAFKA_HOME=/opt/kafka
$KAFKA_HOME/bin/kafka-storage.sh random-uuid > /etc/kafka/cluster-id
UUID=$(cat /etc/kafka/cluster-id)
$KAFKA_HOME/bin/kafka-storage.sh format \
  -t $UUID \
  -c /etc/kafka/server.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon /etc/kafka/server.properties
```

## KRaft Configuration (`server.properties`)

Minimal single-node config:

```properties
# Broker ID
node.id=1

# Listeners
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Controller quorum
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER

# Data directory (FreeBSD: /var/db/kafka-kraft, Linux: /var/lib/kafka)
log.dirs=/var/db/kafka-kraft

# Auto-create topics (dns-flow creates topic via admin API)
auto.create.topics.enable=true

# Default replication (1 for single node)
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

## Cluster Verification

```bash
# Check metadata quorum
$KAFKA_HOME/bin/kafka-metadata-quorum.sh \
  --bootstrap-controller localhost:9093 \
  describe --status

# Output:
# ClusterId:              <UUID>
# LeaderId:               1
# LeaderEpoch:            1
# HighWatermark:          <number>

# List topics
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create topic manually (override 1 partition default)
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --topic dns.raw \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
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

The `/usr/local/etc/rc.d/kafka` file is installed automatically with the `pkg`. Configure in `/etc/rc.conf`:

```
kafka_enable="YES"
kafka_config="/usr/local/etc/kafka/kraft/server.properties"
kafka_data="/var/db/kafka-kraft"
kafka_logs="/var/log/kafka"
```

## Topic & Partition

### Auto-Creation

At startup, if topic `dns.raw` does not exist, dns-flow creates it automatically via the Kafka admin API (1 partition, 1 replication). Suitable for development.

### Create topic manually (production)

For production, create the topic **before** running dns-flow with the desired partition count:

```bash
# 6 partitions, replication-factor 3 (for 3 brokers)
kafka-topics.sh --create \
  --topic dns.raw \
  --partitions 6 \
  --replication-factor 3 \
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
kafka-topics.sh --describe --topic dns.raw --bootstrap-server localhost:9092
```

### Replay from beginning

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group dns-flow --topic dns.raw --reset-offsets --to-earliest --execute
```
