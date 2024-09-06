package main

import (
	"context"
	_ "embed"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/diagnostics"
	"github.com/suessflorian/client-side-analytics/store/duckdb"
)

func main() {
	ctx := context.Background()
	lg := logrus.New()
	lg.SetLevel(logrus.DebugLevel)
	lg.SetFormatter(&logrus.JSONFormatter{})

	d := diagnostics.Begin(ctx, lg)
	ctx = diagnostics.ContextWithDiagnostics(ctx, d)

	connector, err := duckdb.Init(ctx, lg, "duck.db")
	if err != nil {
		lg.WithError(err).Fatal("database connection failure")
	}
	defer connector.Close()

	go func() {
		_, err = generator(ctx, lg, connector, 9_030_000)
		if err != nil {
			lg.WithError(err).Fatal("failed to generate products")
		}
	}()

	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/diagnostics", d.MetricsHandler)
	http.HandleFunc("/script.js", scriptHandler)

	lg.Info("⚡️ http://localhost:8080 ⚡️")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		lg.WithError(err).Info("well... something went wrong")
	}
}
