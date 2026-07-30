package main

import (
	"flag"
	"fmt"
	"log"

	"distributedmq/broker"
	"distributedmq/config"
	"distributedmq/nameserver"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	generateConfig := flag.Bool("generate-config", false, "Generate example config file")
	flag.Parse()

	if *generateConfig {
		config.GenerateConfigExample()
		fmt.Println("Config example generated: config.yaml.example")
		return
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Printf("Loaded config: role=%s, addr=%s, client-addr=%s, nameserver=%s, data-dir=%s\n",
		cfg.Role, cfg.Addr, cfg.ClientAddr, cfg.NameServer, cfg.DataDir)

	switch cfg.Role {
	case "nameserver":
		ns := nameserver.NewNameServer(cfg.Addr, cfg.HTTPPort)
		fmt.Printf("Starting NameServer on :%d\n", cfg.HTTPPort)
		log.Fatal(ns.Start())

	case "broker":
		b := broker.NewBroker(cfg.Addr, cfg.ClientAddr, cfg.HTTPPort, cfg.NameServer, cfg.DataDir)
		b.SetRetention(cfg.RetentionMs, cfg.RetentionBytes)
		fmt.Printf("Starting Broker on %s (client: %s)\n", cfg.Addr, cfg.ClientAddr)
		log.Fatal(b.Start())

	default:
		fmt.Println("Usage: mq -config config.yaml")
		fmt.Println("Or: mq -role nameserver|broker -addr localhost:8080 -client-addr localhost:9092 -nameserver http://localhost:9090 -data-dir ./data")
	}
}