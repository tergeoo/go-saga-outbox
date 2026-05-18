package kafka

import "time"

type ConsumerConfig struct {
	Brokers           []string      `env:"BROKERS"`
	Topic             string        `env:"TOPIC"`
	GroupID           string        `env:"GROUP_ID"`
	MaxBytes          int           `env:"MAX_BYTES" envDefault:"10485760"`   // 10MB
	MaxWait           int           `env:"MAX_WAIT"`                          // 50 ms
	HeartbeatInterval int           `env:"HEARTBEAT_INTERVAL" envDefault:"3"` // 3 seconds
	SessionTimeout    int           `env:"SESSION_TIMEOUT" envDefault:"30"`   // 30 seconds
	RebalanceTimeout  int           `env:"REBALANCE_TIMEOUT" envDefault:"30"` // 30 seconds
	DialerTimeout     int           `env:"DIALER_TIMEOUT" envDefault:"10"`    // 10 seconds
	DialerKeepAlive   int           `env:"DIALOG_KEEP_ALIVE" envDefault:"30"` // 30 seconds
	RetryBackoff      time.Duration `env:"RETRY_BACKOFF" envDefault:"100ms"`
	Auth              AuthConfig
}

type ProducerConfig struct {
	Brokers []string `env:"BROKERS" envDefault:"localhost:19092"`
	Auth    AuthConfig
}

type AuthConfig struct {
	Enabled   bool   `env:"AUTH_ENABLED" envDefault:"false"`
	Mechanism string `env:"AUTH_MECHANISM" envDefault:""` // "scram-sha512", "scram-sha256", "plain"
	Username  string `env:"AUTH_USERNAME"`
	Password  string `env:"AUTH_PASSWORD"`
	TLS       bool   `env:"AUTH_TLS" envDefault:"false"` // false → SASL_PLAINTEXT
	Insecure  bool   `env:"AUTH_INSECURE" envDefault:"false"`
}
