package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

func New(ctx context.Context, lg *logrus.Logger) (*Engine, *Reporter) {
	connection := make(chan update, 10_000_000)

	reporter := &Reporter{
		push: connection,
	}
	engine := &Engine{
		metricsMutex: new(sync.Mutex),
		metrics:      make(map[string][]point),
		queue:        connection,
	}

	go engine.poll(ctx, lg)
	return engine, reporter
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.metricsMutex.Lock()
	defer e.metricsMutex.Unlock()

	res, err := json.Marshal(e.metrics)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (e *Engine) Close(ctx context.Context) error {
	for !e.shutdown.Load() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("could not shut down telemetry engine gracefully")
		default:
			continue
		}
	}
	return nil
}

func (e *Engine) poll(ctx context.Context, lg *logrus.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	bound := 10_000_000
	for {
		select {
		case <-ticker.C:
			var setUpdateBatch []update
			for i := 0; i < bound; i++ {
				select {
				case update := <-e.queue:
					setUpdateBatch = append(setUpdateBatch, update)
				default:
					break
				}
			}

			if len(setUpdateBatch) > 0 {
				e.process(setUpdateBatch...)
				lg.WithFields(logrus.Fields{
					"processed": len(setUpdateBatch),
					"remaining": len(e.queue),
				}).Debug("processed batch of diagnostic updates")
			}
		case <-ctx.Done():
			e.shutdown.Store(true)
			return
		}
	}
}

func (d *Engine) process(batch ...update) {
	d.metricsMutex.Lock()
	defer d.metricsMutex.Unlock()

	for _, update := range batch {
		_, ok := d.metrics[update.label]
		if !ok {
			d.metrics[update.label] = []point{
				{Time: update.Time, Value: update.Value},
			}
		} else {
			last := d.metrics[update.label][len(d.metrics[update.label])-1]
			if squash(last.Time, update.Time) {
				d.metrics[update.label][len(d.metrics[update.label])-1] = point{
					Value: update.Value, Time: last.Time}
			} else {
				d.metrics[update.label] = append(
					d.metrics[update.label],
					point{Time: update.Time, Value: update.Value},
				)
			}
		}
	}
}

func squash(t1, t2 time.Time) bool {
	diff := t1.Sub(t2)
	return diff < time.Second && diff > -time.Second
}

type Engine struct {
	queue        chan update
	metricsMutex *sync.Mutex
	metrics      map[string][]point
	shutdown     atomic.Bool
}
