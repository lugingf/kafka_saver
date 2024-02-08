package kafka

import (
	"context"
	"log/slog"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// KafkaConsumerConfig holds the configuration for the Kafka consumer
type KafkaConsumerConfig struct {
	ClientID      string
	ConsumerGroup string
	BrokerList    []string
}

// KafkaConsumer wraps the Sarama consumer group
type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	MsgChan       chan *KafkaMesasge
}

type KafkaMesasge struct {
	*sarama.ConsumerMessage
}

func NewKafkaConsumer(cfg *KafkaConsumerConfig, msgChan chan *KafkaMesasge) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.ClientID = cfg.ClientID

	consumerGroup, err := sarama.NewConsumerGroup(cfg.BrokerList, cfg.ConsumerGroup, config)
	if err != nil {
		return nil, errors.Wrap(err, "error creating consumer group client")
	}

	return &KafkaConsumer{
		consumerGroup: consumerGroup,
		MsgChan:       msgChan,
	}, nil
}

func (kc *KafkaConsumer) StartConsuming(ctx context.Context, topics []string) {
	consumerGroupHandler := &ConsumerGroupHandler{
		msgChan: kc.MsgChan,
	}

	go func() {
		for {
			err := kc.consumerGroup.Consume(ctx, topics, consumerGroupHandler)
			if err != nil {
				slog.Error("Error from consumer: %v", err)
			}
		}
	}()
}

func (kc *KafkaConsumer) Close() error {
	return kc.consumerGroup.Close()
}

type ConsumerGroupHandler struct {
	msgChan chan *KafkaMesasge
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		kMsg := KafkaMesasge{msg}
		h.msgChan <- &kMsg

		sess.MarkMessage(msg, "")
	}
	return nil
}
