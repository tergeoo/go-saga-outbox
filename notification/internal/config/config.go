package config

import (
	"go-saga-outbox/pkg/db"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/outbox"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Name             string `env:"NAME" envDefault:"notification"`
	MigrationsDir    string `env:"MIGRATIONS_DIR" envDefault:"."`
	MetricsPort      int    `env:"METRICS_PORT" envDefault:"9103"`
	DS               db.Datasource
	OutboxRelay      outbox.RelayConfig   `envPrefix:"OUTBOX_RELAY_"`
	OutboxProducer   kafka.ProducerConfig `envPrefix:"OUTBOX_PRODUCER_"`
	CommandsConsumer kafka.ConsumerConfig `envPrefix:"COMMANDS_CONSUMER_"`
}

func NewConfig() (Config, error) {
	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
