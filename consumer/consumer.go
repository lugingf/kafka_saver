package consumer

import (
	"context"

	"kafka_saver/pkg/kafka"
)

func NewConsumer(cfg *kafka.KafkaConsumerConfig, msgChan chan *kafka.KafkaMesasge) (*KafkaConsumer, error) {
	c, err := kafka.NewKafkaConsumer(cfg, msgChan)
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{consumer: c}, nil
}

type Consumer interface {
	Run(ctx context.Context, topics []string)
	Messages() chan *kafka.KafkaMesasge
	Close() error
}

type KafkaConsumer struct {
	consumer *kafka.KafkaConsumer
}

func (kc *KafkaConsumer) Run(ctx context.Context, topics []string) {
	kc.consumer.StartConsuming(ctx, topics)
}

func (kc *KafkaConsumer) Close() error {
	return kc.consumer.Close()
}

func (kc *KafkaConsumer) Messages() chan *kafka.KafkaMesasge {
	return kc.consumer.MsgChan
}
