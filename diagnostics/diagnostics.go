package diagnostics

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func Begin(ctx context.Context, lg *logrus.Logger) *diagnostics {
	diagnostics := &diagnostics{
		metricsMutex: new(sync.Mutex),
		metrics:      make(map[string][]point),
		queue:        make(chan update, 10_000_000),
	}

	bound := 1_000_000
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				var setUpdateBatch []update
				for i := 0; i < bound; i++ {
					select {
					case update := <-diagnostics.queue:
						setUpdateBatch = append(setUpdateBatch, update)
					default:
						break
					}
				}

				if len(setUpdateBatch) > 0 {
					diagnostics.process(setUpdateBatch...)
					lg.WithFields(logrus.Fields{
						"processed": len(setUpdateBatch),
						"remaining": len(diagnostics.queue),
					}).Debug("processed batch of diagnostic updates")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return diagnostics
}

func ContextWithDiagnostics(ctx context.Context, diag *diagnostics) context.Context {
	return context.WithValue(ctx, contextKey{}, diag)
}

func DiagnosticsFromContext(ctx context.Context) *diagnostics {
	return ctx.Value(contextKey{}).(*diagnostics)
}

func (d *diagnostics) Set(label string, value int) {
	d.queue <- update{
		operation: "SET",
		point: point{
			Time:  time.Now(),
			Value: value,
		},
		label: label,
	}
}

func (d *diagnostics) Add(label string, value int) {
	d.queue <- update{
		operation: "ADD",
		point: point{
			Time:  time.Now(),
			Value: value,
		},
		label: label,
	}
}

func (d *diagnostics) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	d.metricsMutex.Lock()
	defer d.metricsMutex.Unlock()

	res, err := json.Marshal(d.metrics)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (d *diagnostics) process(batch ...update) {
	d.metricsMutex.Lock()
	defer d.metricsMutex.Unlock()

	for _, update := range batch {
		_, ok := d.metrics[update.label]
		if !ok {
			d.metrics[update.label] = []point{
				{Time: update.Time, Value: update.Value},
			}
		} else {
			switch update.operation {
			case "ADD":
				last := d.metrics[update.label][len(d.metrics[update.label])-1]
				if squash(last.Time, update.Time) {
					d.metrics[update.label][len(d.metrics[update.label])-1] = point{
						Value: last.Value + update.Value, Time: last.Time}
				} else {
					d.metrics[update.label] = append(
						d.metrics[update.label],
						point{Time: update.Time, Value: last.Value + update.Value},
					)
				}
			case "SET":
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
}

func squash(t1, t2 time.Time) bool {
	diff := t1.Sub(t2)
	return diff < time.Second && diff > -time.Second
}

type contextKey struct{}

type diagnostics struct {
	queue        chan update
	metricsMutex *sync.Mutex
	metrics      map[string][]point
}

type point struct {
	Time  time.Time `json:"time"`
	Value int       `json:"value"`
}

type update struct {
	point
	label     string
	operation string // TODO: ADD OR SET consts
}
