package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

func generator(ctx context.Context, lg *logrus.Logger, connector *duckdb.Connector, amount int) ([]uuid.UUID, error) {
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

	res := make([]uuid.UUID, 0, amount)
	done := make(chan bool)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		last := 0
		for {
			select {
			case <-ticker.C:
				now := len(res)
				lg.WithField("per_second", now-last).Info("generating products")
				last = now
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < amount; i++ {
		res = append(res, uuid.New())
		uuid := duckdb.UUID{}
		copy(uuid[:], res[i][:])
		err := appender.AppendRow(uuid, "yest", int32(rand.Int()%10_000)+100)
		if err != nil {
			return nil, fmt.Errorf("failed to append row: %w", err)
		}
	}
	done <- true
	return res, nil
}
