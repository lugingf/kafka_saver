package main

import (
	"context"
	"kafka_saver/consumer"
	"os"
	"os/signal"

	log "log/slog"

	"kafka_saver/config"
	ki "kafka_saver/pkg/kafka"
	"kafka_saver/service"
	"kafka_saver/storage"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cfg, err := config.New(ctx)
	if err != nil {
		log.Error("can't initiate config", "error", err)
		return
	}

	logger := log.New(
		log.NewJSONHandler(
			os.Stdout, &log.HandlerOptions{Level: log.LevelInfo},
		),
	)

	metrics := config.InitMetrics(cfg.Monitoring.PromServeAddr)

	consumerOutChan := make(chan *ki.KafkaMesasge)
	cnsmr, err := consumer.NewConsumer(cfg.KafkaInput, consumerOutChan)
	if err != nil {
		logger.Error("can't create consumer", "error", err)
		return
	}
	defer cnsmr.Close()

	strg, err := storage.NewStorage(cfg.DB, logger, metrics)
	if err != nil {
		logger.Error("can't create storage", "error", err)
		return
	}
	defer strg.Close()

	params := service.Params{
		BatchTime:    cfg.App.BatchTime,
		BatchMaxSize: cfg.App.BatchMaxSize,
	}

	srv := service.NewService(params, cnsmr, strg, logger, metrics)

	srv.Run(ctx)
}
