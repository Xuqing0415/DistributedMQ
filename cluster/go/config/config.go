package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Role       string `mapstructure:"role"`
	Addr       string `mapstructure:"addr"`
	ClientAddr string `mapstructure:"client_addr"`
	HTTPPort   int    `mapstructure:"http_port"`
	NameServer string `mapstructure:"nameserver"`
	DataDir    string `mapstructure:"data_dir"`
	RetentionMs int64 `mapstructure:"retention_ms"`
	RetentionBytes int64 `mapstructure:"retention_bytes"`
}

func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "./config.yaml"
	}

	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	viper.AutomaticEnv()
	viper.SetEnvPrefix("MQ")
	viper.BindEnv("role", "MQ_ROLE")
	viper.BindEnv("addr", "MQ_ADDR")
	viper.BindEnv("client_addr", "MQ_CLIENT_ADDR")
	viper.BindEnv("http_port", "MQ_HTTP_PORT")
	viper.BindEnv("nameserver", "MQ_NAMESERVER")
	viper.BindEnv("data_dir", "MQ_DATA_DIR")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config: %v", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	setDefaults(&config)

	return &config, nil
}

func setDefaults(config *Config) {
	if config.Addr == "" {
		config.Addr = "localhost:8080"
	}
	if config.ClientAddr == "" {
		config.ClientAddr = "localhost:9092"
	}
	if config.HTTPPort == 0 {
		config.HTTPPort = 8080
	}
	if config.NameServer == "" {
		config.NameServer = "http://localhost:9090"
	}
	if config.DataDir == "" {
		config.DataDir = "./data"
	}
	if config.RetentionMs == 0 {
		config.RetentionMs = 7 * 24 * 60 * 60 * 1000
	}
}

func GenerateConfigExample() {
	example := `# DistributedMQ Configuration

# Service role: nameserver or broker
role: "broker"

# Raft/gRPC address (used for cluster communication)
addr: "localhost:8081"

# Client TCP address (used for producer/consumer connections)
client_addr: "localhost:9092"

# HTTP port (for admin API)
http_port: 8080

# NameServer URL
nameserver: "http://localhost:9090"

# Data directory for storage
data_dir: "./data"

# Log retention settings
retention_ms: 604800000
retention_bytes: 0
`
	os.WriteFile("config.yaml.example", []byte(example), 0644)
}