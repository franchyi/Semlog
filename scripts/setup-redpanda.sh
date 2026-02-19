#!/bin/bash
set -euo pipefail

if ! command -v rpk &> /dev/null; then
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
  sudo apt-get install -y redpanda
fi

# Newer rpk versions use "development" instead of "dev-container".
sudo rpk redpanda mode development
sudo rpk config set redpanda.advertised_kafka_api '[{address: "127.0.0.1", port: 9092}]'

sudo systemctl start redpanda || sudo rpk redpanda start --detach

echo "Waiting for Redpanda..."
for i in $(seq 1 30); do
  rpk cluster info --brokers 127.0.0.1:9092 >/dev/null 2>&1 && break
  sleep 1
done

echo "Redpanda is running."
rpk cluster info --brokers 127.0.0.1:9092
