package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultGracePeriod   = 10 * time.Second
	defaultChunkBytes    = 15728640
	defaultKafkaMinBytes = 10240
	defaultKafkaMaxBytes = 10485760
	defaultKafkaConns    = 1
	defaultKafkaWorkers  = 8
	offsetFirst          = "first"
	offsetLast           = "last"
)

type (
	Condition struct {
		Key   string `yaml:"Key"`
		Value string `yaml:"Value"`
		Type  string `yaml:"Type"`
		Op    string `yaml:"Op"`
	}

	ElasticSearchConf struct {
		Hosts         []string `yaml:"Hosts"`
		Index         string   `yaml:"Index"`
		DocType       string   `yaml:"DocType"`
		TimeZone      string   `yaml:"TimeZone"`
		MaxChunkBytes int      `yaml:"MaxChunkBytes"`
		Compress      bool     `yaml:"Compress"`
		Username      string   `yaml:"Username"`
		Password      string   `yaml:"Password"`
	}

	Filter struct {
		Action     string      `yaml:"Action"`
		Conditions []Condition `yaml:"Conditions"`
		Fields     []string    `yaml:"Fields"`
		Field      string      `yaml:"Field"`
		Target     string      `yaml:"Target"`
	}

	KafkaConf struct {
		Name       string   `yaml:"Name"`
		Brokers    []string `yaml:"Brokers"`
		Group      string   `yaml:"Group"`
		Topics     []string `yaml:"Topics"`
		Topic      string   `yaml:"Topic"`
		Offset     string   `yaml:"Offset"`
		Conns      int      `yaml:"Conns"`
		Consumers  int      `yaml:"Consumers"`
		Processors int      `yaml:"Processors"`
		MinBytes   int      `yaml:"MinBytes"`
		MaxBytes   int      `yaml:"MaxBytes"`
		Username   string   `yaml:"Username"`
		Password   string   `yaml:"Password"`
		CaFile     string   `yaml:"CaFile"`
	}

	Cluster struct {
		Input struct {
			Kafka KafkaConf `yaml:"Kafka"`
		} `yaml:"Input"`
		Filters []Filter `yaml:"Filters"`
		Output  struct {
			ElasticSearch ElasticSearchConf `yaml:"ElasticSearch"`
		} `yaml:"Output"`
	}

	HTTPConfig struct {
		Addr string `yaml:"Addr"`
	}

	Config struct {
		Clusters    []Cluster     `yaml:"Clusters"`
		GracePeriod time.Duration `yaml:"GracePeriod"`
		HTTP        HTTPConfig    `yaml:"HTTP"`
	}
)

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	if err := cfg.applyDefaults(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c *Config) applyDefaults() error {
	if len(c.Clusters) == 0 {
		return errors.New("config must define at least one cluster")
	}

	if c.GracePeriod <= 0 {
		c.GracePeriod = defaultGracePeriod
	}

	if c.HTTP.Addr == "" {
		c.HTTP.Addr = ":8080"
	}

	for i := range c.Clusters {
		cluster := &c.Clusters[i]
		if err := cluster.Input.Kafka.applyDefaults(); err != nil {
			return fmt.Errorf("cluster %d kafka config: %w", i, err)
		}
		if err := cluster.Output.ElasticSearch.applyDefaults(); err != nil {
			return fmt.Errorf("cluster %d elasticsearch config: %w", i, err)
		}
	}

	return nil
}

func (e *ElasticSearchConf) applyDefaults() error {
	if len(e.Hosts) == 0 {
		return errors.New("elasticsearch hosts are required")
	}
	if e.Index == "" {
		return errors.New("elasticsearch index is required")
	}
	if e.DocType == "" {
		e.DocType = "doc"
	}
	if e.MaxChunkBytes <= 0 {
		e.MaxChunkBytes = defaultChunkBytes
	}
	return nil
}

func (k *KafkaConf) applyDefaults() error {
	if len(k.Topics) == 0 && k.Topic != "" {
		k.Topics = []string{k.Topic}
	}
	if len(k.Brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if len(k.Topics) == 0 {
		return errors.New("at least one kafka topic is required")
	}
	if k.Group == "" {
		return errors.New("kafka consumer group is required")
	}
	if k.Offset == "" {
		k.Offset = offsetLast
	}
	if k.Offset != offsetFirst && k.Offset != offsetLast {
		return fmt.Errorf("unsupported offset value %q", k.Offset)
	}
	if k.Conns <= 0 {
		k.Conns = defaultKafkaConns
	}
	if k.Consumers <= 0 {
		k.Consumers = defaultKafkaWorkers
	}
	if k.Processors <= 0 {
		k.Processors = defaultKafkaWorkers
	}
	if k.MinBytes <= 0 {
		k.MinBytes = defaultKafkaMinBytes
	}
	if k.MaxBytes <= 0 {
		k.MaxBytes = defaultKafkaMaxBytes
	}
	return nil
}
