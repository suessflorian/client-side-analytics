package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/diagnostics"
)

const (
	DIAGNOSTIC_GENERATED_PRODUCTS = "generated_products"
)

func generator(ctx context.Context, lg *logrus.Logger, connector *duckdb.Connector, amount int) ([]uuid.UUID, error) {
	res := make([]uuid.UUID, 0, amount)
	defer func() {
		lg.WithField("quantity", len(res)).Info("generated products flushed to disc")
	}()

	conn, err := connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "products")
	if err != nil {
		return nil, fmt.Errorf("failed establish appender: %w", err)
	}
	defer appender.Close()

	for i := 0; i < amount; i++ {
		res = append(res, uuid.New())
		uuid := duckdb.UUID{}
		copy(uuid[:], res[i][:])
		err := appender.AppendRow(uuid, "yest", int32(rand.Int()%10_000)+100)
		if err != nil {
			return nil, fmt.Errorf("failed to append row: %w", err)
		}
		go diagnostics.DiagnosticsFromContext(ctx).Add(DIAGNOSTIC_GENERATED_PRODUCTS, 1)
	}

	return res, nil
}
