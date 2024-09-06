package main

import (
	"context"
	_ "embed"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/store/duckdb"
)

func main() {
	ctx := context.Background()
	lg := logrus.New()
	lg.SetFormatter(&logrus.JSONFormatter{})

	connector, err := duckdb.Init(ctx, lg, "duck.db")
	if err != nil {
		lg.WithError(err).Fatal("database connection failure")
	}
	defer connector.Close()

	_, err = generator(ctx, lg, connector, 10_000_000)
	if err != nil {
		lg.WithError(err).Fatal("failed to generate products")
	}

	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/script.js", scriptHandler)

	lg.Info("⚡️ http://localhost:8080 ⚡️")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		lg.WithError(err).Info("well... something went wrong")
	}
}
