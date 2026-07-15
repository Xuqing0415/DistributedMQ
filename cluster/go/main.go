package main

import (
	"flag"
	"fmt"
	"log"

	"distributedmq/broker"
	"distributedmq/nameserver"
)

func main() {
	role := flag.String("role", "", "nameserver or broker")
	addr := flag.String("addr", "localhost:8080", "server address")
	httpPort := flag.Int("http-port", 8080, "HTTP port")
	nameserverURL := flag.String("nameserver", "http://localhost:9090", "NameServer URL")
	dataDir := flag.String("data-dir", "./data", "data directory")

	flag.Parse()

	switch *role {
	case "nameserver":
		ns := nameserver.NewNameServer(*addr, *httpPort)
		fmt.Printf("Starting NameServer on :%d\n", *httpPort)
		log.Fatal(ns.Start())

	case "broker":
		peers := []string{"localhost:8081", "localhost:8082", "localhost:8083"}
		b := broker.NewBroker(*addr, *nameserverURL, *dataDir, peers)
		fmt.Printf("Starting Broker on %s\n", *addr)
		log.Fatal(b.Start())

	default:
		fmt.Println("Usage: go run main.go -role nameserver|broker [options]")
	}
}
