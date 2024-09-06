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
		set:          make(chan update, 10_000_000),
		add:          make(chan update, 10_000_000),
	}

	bound := 1_000_000
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		select {
		case <-ticker.C:
			lg.WithFields(logrus.Fields{
				"set_update_queue": len(diagnostics.set),
				"add_update_queue": len(diagnostics.add),
			}).Debug("processing diagnostic updates")

			var addUpdateBatch []update
			for i := 0; i < bound; i++ {
				select {
				case update := <-diagnostics.add:
					addUpdateBatch = append(addUpdateBatch, update)
				default:
					break
				}
			}
			diagnostics.processAddUpdates(addUpdateBatch...)

			var setUpdateBatch []update
			for i := 0; i < bound; i++ {
				select {
				case update := <-diagnostics.set:
					setUpdateBatch = append(setUpdateBatch, update)
				default:
					break
				}
			}
			diagnostics.processSetUpdates(setUpdateBatch...)
			lg.Debug("batch of updates processed")
		case <-ctx.Done():
			return
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
	d.set <- update{
		point: point{
			Time:  time.Now(),
			Value: value,
		},
		label: label,
	}
}

func (d *diagnostics) Add(label string, value int) {
	d.add <- update{
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

func (d *diagnostics) processAddUpdates(batch ...update) {
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
			d.metrics[update.label] = append(
				d.metrics[update.label],
				point{Time: update.Time, Value: last.Value + update.Value},
			)
		}
	}
}

func (d *diagnostics) processSetUpdates(batch ...update) {
	d.metricsMutex.Lock()
	defer d.metricsMutex.Unlock()

	for _, update := range batch {
		_, ok := d.metrics[update.label]
		if !ok {
			d.metrics[update.label] = make([]point, 0, 1)
		}
		d.metrics[update.label] = append(
			d.metrics[update.label],
			point{Time: update.Time, Value: update.Value},
		)
	}
}

type contextKey struct{}

type diagnostics struct {
	add          chan update
	set          chan update
	metricsMutex *sync.Mutex
	metrics      map[string][]point
}

type point struct {
	Time  time.Time `json:"time"`
	Value int       `json:"value"`
}

type update struct {
	point
	label string
}
