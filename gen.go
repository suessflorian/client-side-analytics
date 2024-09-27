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
	DIAGNOSTIC_GENERATED_MERCHANTS         = "Generated merchants"
)

func generator(ctx context.Context, lg *logrus.Logger, connector *duckdb.Connector) error {
	conn, err := connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	merchants, err := generateMerchants(ctx, lg, conn, 1)
	if err != nil {
		return err
	}

	for _, merchant := range merchants {
		products, err := generateProducts(ctx, lg, conn, merchant, rand.Int()%100)
		if err != nil {
			return err
		}

		transactions, err := generateTransactions(ctx, lg, conn, merchant, rand.Int()%100_000)
		if err != nil {
			return err
		}

		err = generateTransactionLines(ctx, lg, conn, merchant, products, transactions, len(transactions)*7)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateMerchants(ctx context.Context, lg *logrus.Logger, conn driver.Conn, amount int) ([]uuid.UUID, error) {
	appender, err := duckdb.NewAppenderFromConn(conn, "", "merchants")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for merchants: %w", err)
	}
	defer lg.WithField("quantity", amount).Debug("generated merchants flushed to disk")
	defer appender.Close()

	var names = []string{
		"Tech",
		"Spark",
		"Volt",
		"Nano",
		"Sync",
		"Proto",
		"Quantum",
		"Byte",
		"Pulse",
		"Gear",
		"Hex",
		"Vibe",
		"Echo",
		"Glide",
		"Nex",
		"Flex",
		"Optic",
		"Circuit",
		"Zylo",
		"Fusion",
	}

	var postfixes = []string{
		"Co.",
		"Ltd.",
		"Inc.",
		"Corporation",
		"LLC",
		"GmbH",
		"Enterprises",
		"Industries",
		"Solutions",
		"Group",
	}

	merchants := make([]uuid.UUID, amount)
	for i := 0; i < amount; i++ {
		merchants[i] = uuid.New()
		if err := appender.AppendRow(
			duckdb.UUID(merchants[i]),
			names[rand.Int()%len(names)]+names[rand.Int()%len(names)]+" "+postfixes[rand.Int()%len(postfixes)],
		); err != nil {
			return nil, fmt.Errorf("failed to append merchant row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_MERCHANTS, i+1)
	}

	return merchants, nil
}

func generateProducts(ctx context.Context, lg *logrus.Logger, conn driver.Conn, merchant uuid.UUID, amount int) ([]uuid.UUID, error) {
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
			duckdb.UUID(merchant),
		); err != nil {
			return nil, fmt.Errorf("failed to append product row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_PRODUCTS, i+1)
	}

	return products, nil
}

func generateTransactions(ctx context.Context, lg *logrus.Logger, conn driver.Conn, merchant uuid.UUID, amount int) ([]uuid.UUID, error) {
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
			duckdb.UUID(merchant),
		); err != nil {
			return nil, fmt.Errorf("failed to append transaction row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTIONS, i+1)
	}

	return transactions, nil
}

func generateTransactionLines(ctx context.Context, lg *logrus.Logger, conn driver.Conn, merchant uuid.UUID, products, transactions []uuid.UUID, amount int) error {
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
			duckdb.UUID(merchant),
		); err != nil {
			return fmt.Errorf("failed to append transaction line row: %w", err)
		}
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTION_LINES, i+1)
	}

	return nil
}
