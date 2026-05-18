package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"go-saga-outbox/pkg/kafka"
	"go-saga-outbox/pkg/messaging"
	"strconv"
)

type Producer struct {
	kafka *kafka.Producer
}

func NewOutboxProducer(cfg kafka.ProducerConfig) *Producer {
	return &Producer{
		kafka: kafka.NewProducer(cfg),
	}
}

func (p *Producer) Publish(ctx context.Context, msg Outbox) error {
	key := []byte(msg.Key.String())

	var hdr messaging.MessageHeaders
	if err := json.Unmarshal(msg.Headers, &hdr); err != nil {
		return fmt.Errorf("unmarshal headers: %w", err)
	}

	kafkaHeaders := map[string][]byte{
		"message_id":     []byte(hdr.MessageID.String()),
		"saga_id":        []byte(hdr.SagaID.String()),
		"event_type":     []byte(hdr.EventType),
		"schema_version": []byte(strconv.Itoa(hdr.SchemaVersion)),
	}
	if hdr.CausationID != nil {
		kafkaHeaders["causation_id"] = []byte(hdr.CausationID.String())
	}

	return p.kafka.Send(ctx, msg.Topic, key, msg.Payload, kafkaHeaders)
}

func (p *Producer) Shutdown(ctx context.Context) error {
	return p.kafka.Shutdown(ctx)
}
