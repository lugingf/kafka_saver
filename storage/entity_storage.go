package storage

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"kafka_saver/config"
	"kafka_saver/dto"
	"kafka_saver/pkg/db"
)

type Storage interface {
	Save(context.Context, []dto.Entity) (int, error)
	Close() error
}

type EntityStorage struct {
	db      *sqlx.DB
	logger  *log.Logger
	metrics *config.Metrics
}

func NewStorage(cfg *db.DB, logger *log.Logger, metrics *config.Metrics) (Storage, error) {
	cdbx, err := db.OpenSQLXConn(*cfg)
	if err != nil {
		return nil, errors.Wrap(err, "can't OpenSQLXConn")
	}
	return &EntityStorage{
		db:      cdbx,
		logger:  logger,
		metrics: metrics,
	}, nil
}

func (s *EntityStorage) Save(ctx context.Context, values []dto.Entity) (int, error) {
	q, vals := fmtInsertQuery(sqlInsertEntity, values)
	result := make([]dto.Entity, 0)

	queryBegin := time.Now()
	err := s.db.SelectContext(ctx, &result, q, vals...)
	s.metrics.QueryDurationObserve("sqlInsertEntity", queryBegin)

	return len(result), errors.Wrapf(err, "fail to run %s", "sqlInsertEntity")
}

func (s *EntityStorage) Close() error {
	return s.db.Close()
}

func fmtInsertQuery(query string, es []dto.Entity) (q string, vals []interface{}) {
	vi := make([]string, 0)

	for i := 0; i < len(es); i++ {
		vj := make([]string, 0)
		for j := 0; j < es[i].ColumnsNum(); j++ {
			vj = append(vj, fmt.Sprintf("$%d", i*es[i].ColumnsNum()+j+1))
		}
		vi = append(vi, fmt.Sprintf("(%s)", strings.Join(vj, ",")))
		vals = append(vals, es[i].Columns()...)
	}

	q = fmt.Sprintf(query, strings.Join(vi, ","))

	return
}
