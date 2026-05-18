package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

type Producer struct {
	writer *kafka.Writer
	config ProducerConfig
}

func NewProducer(cfg ProducerConfig) *Producer {
	var mech sasl.Mechanism
	var err error

	if cfg.Auth.Enabled {
		mech, err = newScramMechanism(cfg.Auth)

		if err != nil {
			slog.Error("failed to create SASL mechanism", "err", err)
			return nil
		}
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: -1,
		Async:        false,
		Transport: &kafka.Transport{
			SASL: mech,
		},
	}

	return &Producer{
		writer: w,
		config: cfg,
	}
}

func (p *Producer) Send(ctx context.Context, topic string, key []byte, value []byte, headers map[string][]byte) error {
	headersArr := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		headersArr = append(headersArr, kafka.Header{
			Key:   k,
			Value: v,
		})
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headersArr,
		Time:    time.Now(),
	}

	slog.Info("send message", "key", key, "value", value)

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write kafka message: %w", err)
	}

	return nil
}

func (p *Producer) Shutdown(_ context.Context) error {
	slog.Info("shutting down kafka producer",
		"brokers", p.config.Brokers)

	return p.writer.Close()
}
