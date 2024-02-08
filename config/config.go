package config

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sethvargo/go-envconfig"

	"kafka_saver/pkg/db"
	"kafka_saver/pkg/kafka"
)

type Config struct {
	App        App                        `env:",prefix=APP_"`
	DB         *db.DB                     `env:",prefix=DB_CORE_"`
	KafkaInput *kafka.KafkaConsumerConfig `env:",prefix=INPUT_"`
	Monitoring *Monitoring                `env:",prefix=MONITORING_"`
}

type App struct {
	BatchTime     time.Duration `env:"BATCH_TIME,default=30s"`
	BatchMaxSize  int           `env:"BATCH_SIZE,default=100"`
	ConsumerGroup string        `env:"CONSUMER_GROUP,default=kafka_saver"`
	Topic         string        `env:"INPUT_TOPIC,default=input_topic"`
}

type Monitoring struct {
	PromServeAddr string `env:"PROMETHEUS_PORT"`
}

func New(ctx context.Context) (Config, error) {
	cfg := Config{}

	if err := envconfig.Process(ctx, &cfg); err != nil {
		return cfg, errors.Wrap(err, "can't process envconfig")
	}

	if cfg.KafkaInput.ClientID == "" {
		hst, err := os.Hostname()
		if err != nil {
			return cfg, errors.Wrap(err, "can't get hostname for config")
		}

		cfg.KafkaInput.ClientID = fmt.Sprintf("kafka_saver_%s", hst)
	}

	return cfg, nil
}
