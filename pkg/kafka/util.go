package kafka

import (
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"log/slog"
)

func newScramMechanism(cfg AuthConfig) (sasl.Mechanism, error) {
	var mech sasl.Mechanism
	var err error

	username := cfg.Username
	password := cfg.Password

	switch cfg.Mechanism {
	case "scram-sha512":
		mech, err = scram.Mechanism(scram.SHA512, username, password)

	case "scram-sha256":
		mech, err = scram.Mechanism(scram.SHA256, username, password)

	case "plain":
		mech, err = plain.Mechanism{
			Username: username,
			Password: password,
		}, nil

	default:
		slog.Info("unknown SASL mechanism", "mech", cfg.Mechanism)
		return nil, nil
	}

	return mech, err
}
