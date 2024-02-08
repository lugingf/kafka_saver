package config

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const SummaryAgeBuckets = uint32(3)

// SummaryMaxAge
// MaxAge defines the duration for which an observation stays relevant
// for the summary. Only applies to pre-calculated quantiles, does not
// apply to _sum and _count. Must be positive. The default value is
// DefMaxAge time.Duration = 10 * time.Minute
const SummaryMaxAge = 120 * time.Second

// SummaryObjectives
// Objectives defines the quantile rank estimates with their respective
// absolute error. If Objectives[q] = e, then the value reported for q
// will be the φ-quantile value for some φ between q-e and q+e.  The
// default value is an empty map, resulting in a summary without
// quantiles.
func SummaryObjectives() map[float64]float64 {
	return map[float64]float64{
		0.5:  0.5,
		0.9:  0.9,
		0.95: 0.95,
		0.99: 0.99,
		1:    1,
	}
}

type Metrics struct {
	registerer            sync.Once
	consumedMessagesCount *prometheus.CounterVec
	savedEntitiesCount    *prometheus.CounterVec
	queryDuration         *prometheus.SummaryVec
}

var metrics = Metrics{
	registerer: sync.Once{},
}

const appName = "kafka_saver"
const labelApp = "app"

func InitMetrics(promServerAddr string) *Metrics {
	metrics.consumedMessagesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumed_messages_count",
			Help: "Number of consumed messages",
		},
		[]string{labelApp},
	)

	metrics.savedEntitiesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "saved_new_entities_count",
			Help: "Number updated entities",
		},
		[]string{labelApp},
	)

	metrics.queryDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "query_duration",
			Help:       "QueryDuration",
			Objectives: SummaryObjectives(),
			AgeBuckets: SummaryAgeBuckets,
			MaxAge:     SummaryMaxAge,
		},
		[]string{"name", labelApp},
	)
	metrics.registerAndRun(promServerAddr)

	return &metrics
}

func (s *Metrics) registerAndRun(promServerAddr string) {
	s.registerer.Do(func() {
		prometheus.MustRegister(metrics.consumedMessagesCount)
		prometheus.MustRegister(metrics.queryDuration)
		prometheus.MustRegister(metrics.savedEntitiesCount)

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			err := http.ListenAndServe(promServerAddr, nil)
			if err != nil {
				log.Error("can't start metrics server", "error", err.Error())
			}
		}()
	})
}

func (s *Metrics) ConsumedMessagesCountInc() {
	s.consumedMessagesCount.WithLabelValues(appName).Inc()
}

func (s *Metrics) ConsumedMessagesCountAdd(value int) {
	s.consumedMessagesCount.WithLabelValues(appName).Add(float64(value))
}

func (s *Metrics) QueryDurationObserve(name string, startTime time.Time) {
	s.queryDuration.WithLabelValues(name, appName).Observe(time.Since(startTime).Seconds())
}
