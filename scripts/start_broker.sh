#!/bin/bash
cd "$(dirname "$0")/../cluster/go"
go run main.go -role broker -addr localhost:8081 -nameserver http://localhost:9090 -data-dir ./data/broker1
