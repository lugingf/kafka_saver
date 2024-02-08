package service

import (
	"context"
	"encoding/json"
	log "log/slog"
	"sync"
	"time"

	"kafka_saver/config"
	"kafka_saver/consumer"
	"kafka_saver/dto"
	"kafka_saver/storage"
)

type Service struct {
	consumer consumer.Consumer
	storage  storage.Storage
	logger   *log.Logger
	metrics  *config.Metrics
	cfg      Params
}

type Params struct {
	BatchTime    time.Duration
	BatchMaxSize int
}

func NewService(serviceParams Params, consumer consumer.Consumer, storage storage.Storage, logger *log.Logger, metrics *config.Metrics) *Service {
	return &Service{
		consumer: consumer,
		storage:  storage,
		logger:   logger,
		metrics:  metrics,
		cfg:      serviceParams,
	}
}

func (s *Service) Run(ctx context.Context) {
	entityChan := make(chan dto.Entity)
	defer close(entityChan)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		// тут мы запускаем отдельный воркер, чтобы слушать сообщения из Kafka конвертить их в нашу сущность
		s.consume(ctx, entityChan)
		wg.Done()
	}()

	go func() {
		// а тут мы уже сконверченные сообщения из кафки собираем по пачкам и сохраняем в базу данных
		s.collectBatchAndSave(ctx, entityChan)
		wg.Done()
	}()

	wg.Wait()
}

func (s *Service) consume(ctx context.Context, entities chan dto.Entity) {
	for {
		select {
		case msg := <-s.consumer.Messages():
			s.metrics.ConsumedMessagesCountInc()

			msgBody := msg.Value
			Entity := dto.Entity{}

			err := json.Unmarshal(msgBody, &Entity)
			if err != nil {
				s.logger.Error("consume: can't unmarshal message", "error", err, "msgValue", string(msgBody))
				continue
			}

			entities <- Entity

		case <-ctx.Done():
			s.logger.Warn("consume: got context Done")
			return
		}
	}
}

func (s *Service) collectBatchAndSave(ctx context.Context, entityChan chan dto.Entity) {
	entityBatch := make(map[string]dto.Entity)
	ticker := time.NewTicker(s.cfg.BatchTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if len(entityBatch) < 1 {
				continue
			}

			s.persist(ctx, entityBatch)
			entityBatch = make(map[string]dto.Entity)

		case entity := <-entityChan:
			entityBatch[entity.EntID] = entity
			if len(entityBatch) < s.cfg.BatchMaxSize {
				continue
			}

			s.persist(ctx, entityBatch)
			// clean batch
			entityBatch = make(map[string]dto.Entity)

		case <-ctx.Done():
			s.logger.Warn("collectBatchAndSave: got context Done")
			return
		}
	}
}

func (s *Service) persist(ctx context.Context, entityBatch map[string]dto.Entity) {
	batchArray := make([]dto.Entity, 0, len(entityBatch))
	for _, entity := range entityBatch {
		batchArray = append(batchArray, entity)
	}

	countSaved, err := s.storage.Save(ctx, batchArray)
	if err != nil {
		s.logger.Error("collectBatchAndSave: can't save batch entities scores", "error", err)
		return
	}

	if countSaved > 0 {
		s.logger.Info("entities saved", "count", countSaved)
		s.metrics.ConsumedMessagesCountAdd(countSaved)
	}
}
