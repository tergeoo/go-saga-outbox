package config

import (
	"go-saga-outbox/pkg/db"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/outbox"
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Name   string `env:"ORCHESTRATOR_NAME" envDefault:"orchestrator"`
	Server struct {
		Port    int           `env:"PORT" envDefault:"8086"`
		Timeout time.Duration `env:"TIMEOUT" envDefault:"30s"`
	} `envPrefix:"SERVER_"`
	MigrationsDir        string `env:"MIGRATIONS_DIR" envDefault:"."`
	DS                   db.Datasource
	OutboxRelay          outbox.RelayConfig   `envPrefix:"OUTBOX_RELAY_"`
	OutboxProducer       kafka.ProducerConfig `envPrefix:"OUTBOX_PRODUCER_"`
	InventoryConsumer    kafka.ConsumerConfig `envPrefix:"INVENTORY_CONSUMER_"`
	PaymentConsumer      kafka.ConsumerConfig `envPrefix:"PAYMENT_CONSUMER_"`
	NotificationConsumer kafka.ConsumerConfig `envPrefix:"NOTIFICATION_CONSUMER_"`
	RepublisherInterval  time.Duration        `env:"REPUBLISHER_INTERVAL" envDefault:"1s"`
}

func NewConfig() (Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
