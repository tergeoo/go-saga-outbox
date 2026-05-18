package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

var ErrMalformedHeaders = errors.New("kafka: malformed headers")

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	MessageID uuid.UUID
}

func (m *Message) GetSagaIDFromHeaders() (uuid.UUID, error) {
	if m.Headers == nil || len(m.Headers) == 0 {
		return uuid.Nil, fmt.Errorf("%w: empty", ErrMalformedHeaders)
	}
	sagaID, ok := m.Headers["saga_id"]
	if !ok {
		return uuid.Nil, fmt.Errorf("%w: saga_id missing", ErrMalformedHeaders)
	}
	id, err := uuid.Parse(sagaID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: saga_id invalid: %s", ErrMalformedHeaders, sagaID)
	}

	return id, nil
}

type Handler func(ctx context.Context, msg Message) error

type Consumer struct {
	config ConsumerConfig
	reader *kafka.Reader
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	var mech sasl.Mechanism
	var err error

	if cfg.Auth.Enabled {
		mech, err = newScramMechanism(cfg.Auth)
		if err != nil {
			slog.Error("failed to build SASL mechanism", "err", err)
			return nil
		}
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		Topic:             cfg.Topic,
		GroupID:           cfg.GroupID,
		MinBytes:          1,
		MaxBytes:          cfg.MaxBytes,
		MaxWait:           time.Duration(cfg.MaxWait) * time.Millisecond,
		HeartbeatInterval: time.Duration(cfg.HeartbeatInterval) * time.Second,
		SessionTimeout:    time.Duration(cfg.SessionTimeout) * time.Second,
		RebalanceTimeout:  time.Duration(cfg.RebalanceTimeout) * time.Second,

		Dialer: &kafka.Dialer{
			Timeout:       time.Duration(cfg.DialerTimeout) * time.Second,
			KeepAlive:     time.Duration(cfg.DialerKeepAlive) * time.Second,
			SASLMechanism: mech, // <── авторизация
		},
	})

	c := &Consumer{
		config: cfg,
		reader: r,
	}

	return c
}

func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	backoff := c.config.RetryBackoff
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("fetch failed", "err", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
			if backoff < 2*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = c.config.RetryBackoff

		parsed := toMessage(m)
		if err := handler(ctx, parsed); err != nil {
			if errors.Is(err, ErrPermanent) {
				slog.Error("permanent handler error, committing", "topic", parsed.Topic, "offset", parsed.Offset, "err", err)
				if commitErr := c.reader.CommitMessages(ctx, m); commitErr != nil {
					slog.Error("commit after permanent failed", "err", commitErr)
				}
				continue
			}
		}
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			slog.Error("commit failed", "err", err, "topic", parsed.Topic, "offset", parsed.Offset)
			continue
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func toMessage(msg kafka.Message) Message {
	h := make(map[string]string, len(msg.Headers))
	for _, kv := range msg.Headers {
		h[kv.Key] = string(kv.Value)
	}
	var msgID uuid.UUID
	if v, ok := h["message_id"]; ok {
		id, err := uuid.Parse(v)
		if err != nil {
			slog.Error("failed to parse message_id", "err", err)
		}
		msgID = id
	}

	return Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   h,
		MessageID: msgID,
	}
}
