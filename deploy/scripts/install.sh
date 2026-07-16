#!/bin/bash

set -e

MQ_DIR="/opt/mq"
CONFIG_DIR="$MQ_DIR/config"
DATA_DIR="$MQ_DIR/data"
BIN_DIR="$MQ_DIR/bin"
LIB_DIR="$MQ_DIR/lib"
LOG_DIR="/var/log/mq"

echo "=== DistributedMQ Installation ==="

echo "1. Creating system user..."
if ! id mq &>/dev/null; then
    useradd -r -s /sbin/nologin mq
fi

echo "2. Creating directories..."
mkdir -p $CONFIG_DIR
mkdir -p $DATA_DIR
mkdir -p $BIN_DIR
mkdir -p $LIB_DIR
mkdir -p $LOG_DIR

echo "3. Copying configuration files..."
cp deploy/config/*.yaml $CONFIG_DIR/

echo "4. Copying systemd service files..."
cp deploy/systemd/*.service /etc/systemd/system/

echo "5. Copying logrotate config..."
cp deploy/logrotate/mq /etc/logrotate.d/mq

echo "6. Building binary..."
cd cluster/go
go build -o $BIN_DIR/mq .

echo "7. Setting permissions..."
chown -R mq:mq $MQ_DIR
chown -R mq:mq $LOG_DIR

echo "8. Reloading systemd..."
systemctl daemon-reload

echo "9. Enabling services..."
systemctl enable mq-nameserver
systemctl enable mq-broker@1
systemctl enable mq-broker@2
systemctl enable mq-broker@3

echo ""
echo "Installation complete!"
echo ""
echo "To start the cluster:"
echo "  systemctl start mq-nameserver"
echo "  systemctl start mq-broker@{1..3}"
echo ""
echo "To check status:"
echo "  systemctl status mq-nameserver"
echo "  journalctl -u mq-nameserver -f"
echo "  journalctl -u mq-broker@1 -f"