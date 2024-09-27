package telemetry

import (
	"time"
)

type Reporter struct {
	push chan update
}

type update struct {
	point
	label string
}

type point struct {
	Time  time.Time `json:"time"`
	Value any       `json:"value"`
}

func (d *Reporter) Set(label string, value any) {
	d.push <- update{
		point: point{
			Time:  time.Now(),
			Value: value,
		},
		label: label,
	}
}
