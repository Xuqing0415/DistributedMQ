#!/bin/bash
cd "$(dirname "$0")/../cluster/go"
go run main.go -role nameserver -http-port 9090
