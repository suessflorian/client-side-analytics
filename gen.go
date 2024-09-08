package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"github.com/suessflorian/client-side-analytics/diagnostics"
)

const (
	DIAGNOSTIC_GENERATED_TRANSACTIONS      = "Generated transactions"
	DIAGNOSTIC_GENERATED_TRANSACTION_LINES = "Generated transaction lines"
	DIAGNOSTIC_GENERATED_PRODUCTS          = "Generated products"
)

func generator(ctx context.Context, lg *logrus.Logger, connector *duckdb.Connector) error {
	conn, err := connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	products, err := generateProducts(ctx, lg, conn, 20)
	if err != nil {
		return err
	}

	transactions, err := generateTransactions(ctx, lg, conn, 10_000_000)
	if err != nil {
		return err
	}

	err = generateTransactionLines(ctx, lg, conn, products, transactions, 100_000_000)
	if err != nil {
		return err
	}

	return nil
}

func generateProducts(ctx context.Context, lg *logrus.Logger, conn driver.Conn, amount int) ([]uuid.UUID, error) {
	appender, err := duckdb.NewAppenderFromConn(conn, "", "products")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for products: %w", err)
	}
	defer lg.WithField("quantity", amount).Debug("generated products flushed to disk")
	defer appender.Close()

	var names = []string{
		"Gear",
		"Widget",
		"Cog",
		"Circuit",
		"Gizmo",
		"Module",
		"Bolt",
		"Spring",
		"Lever",
		"Crank",
		"Rotor",
		"Piston",
		"Valve",
		"Switch",
		"Spark",
		"Servo",
		"Pulley",
		"Ratchet",
		"Sprocket",
		"Nodule",
	}

	products := make([]uuid.UUID, amount)
	for i := 0; i < amount; i++ {
		products[i] = uuid.New()
		if err := appender.AppendRow(
			duckdb.UUID(products[i]),
			names[rand.Int()%len(names)]+" "+names[rand.Int()%len(names)],
			rand.Int31()%10_000+100,
		); err != nil {
			return nil, fmt.Errorf("failed to append product row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_PRODUCTS, i+1)
	}

	return products, nil
}

func generateTransactions(ctx context.Context, lg *logrus.Logger, conn driver.Conn, amount int) ([]uuid.UUID, error) {
	appender, err := duckdb.NewAppenderFromConn(conn, "", "transactions")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for transactions: %w", err)
	}
	defer lg.WithField("quantity", amount).Debug("generated transactions flushed to disk")
	defer appender.Close()

	transactions := make([]uuid.UUID, amount)
	now := time.Now()
	for i := 0; i < amount; i++ {
		transactions[i] = uuid.New()
		if err := appender.AppendRow(
			duckdb.UUID(transactions[i]),
			now.Add(-time.Duration(rand.Int())*time.Hour),
		); err != nil {
			return nil, fmt.Errorf("failed to append transaction row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTIONS, i+1)
	}

	return transactions, nil
}

func generateTransactionLines(ctx context.Context, lg *logrus.Logger, conn driver.Conn, products, transactions []uuid.UUID, amount int) error {
	appender, err := duckdb.NewAppenderFromConn(conn, "", "transaction_lines")
	if err != nil {
		return fmt.Errorf("failed to establish appender for transaction lines: %w", err)
	}
	defer lg.WithField("quantity", amount).Debug("generated transaction lines flushed to disk")
	defer appender.Close()

	for i := 0; i < amount; i++ {
		if err := appender.AppendRow(
			duckdb.UUID(transactions[rand.Int()%len(transactions)]),
			duckdb.UUID(products[rand.Int()%len(products)]),
			int32(rand.Int()%13),
		); err != nil {
			return fmt.Errorf("failed to append transaction line row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTION_LINES, i+1)
	}

	return nil
}
