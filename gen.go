package main

import (
	"context"
	"database/sql"
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

type gen struct {
	// stats keeps track of how many different entities have been generated.
	stats struct {
		merchants    int
		products     int
		transactions int
		lines        int
	}
	connector *duckdb.Connector
}

func newGenerator(ctx context.Context, connector *duckdb.Connector) (*gen, error) {
	g := &gen{connector: connector}

	for table, count := range map[string]*int{
		"merchants":         &g.stats.merchants,
		"products":          &g.stats.products,
		"transactions":      &g.stats.transactions,
		"transaction_lines": &g.stats.lines,
	} {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		if err := sql.OpenDB(g.connector).QueryRowContext(ctx, query).Scan(count); err != nil {
			return nil, fmt.Errorf("failed to get row count for table %s: %w", table, err)
		}
	}

	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_MERCHANTS, g.stats.merchants)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_PRODUCTS, g.stats.products)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTIONS, g.stats.transactions)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTION_LINES, g.stats.lines)

	return g, nil
}

func (g *gen) create(ctx context.Context, lg *logrus.Logger, amount int) error {
  lg.Info("generator running")

	merchants, err := g.merchants(ctx, lg, amount)
	if err != nil {
		return err
	}

	for _, merchant := range merchants {
		products, err := g.products(ctx, lg, merchant, rand.Int()%100)
		if err != nil {
			return err
		}

		transactions, err := g.transactions(ctx, lg, merchant, rand.Int()%10_000)
		if err != nil {
			return err
		}

		err = g.lines(ctx, lg, merchant, products, transactions, len(transactions)*7)
		if err != nil {
			return err
		}
	}

  lg.Info("generator idle")
	return nil
}

func (g *gen) merchants(ctx context.Context, lg *logrus.Logger, amount int) ([]uuid.UUID, error) {
	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "merchants")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for merchants: %w", err)
	}
	defer lg.WithField("quantity", amount).Info("flushing merchants to disk")
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
		g.stats.merchants++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_MERCHANTS, g.stats.merchants)
	}

	return merchants, nil
}

func (g *gen) products(ctx context.Context, lg *logrus.Logger, merchant uuid.UUID, amount int) ([]uuid.UUID, error) {
	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "products")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for products: %w", err)
	}
	defer lg.WithField("quantity", amount).Info("flushing products to disk")
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
		g.stats.products++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_PRODUCTS, g.stats.products)
	}

	return products, nil
}

func (g *gen) transactions(ctx context.Context, lg *logrus.Logger, merchant uuid.UUID, amount int) ([]uuid.UUID, error) {
	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "transactions")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for transactions: %w", err)
	}
	defer lg.WithField("quantity", amount).Info("flushing transactions to disk")
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
		g.stats.transactions++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTIONS, g.stats.transactions)
	}

	return transactions, nil
}

func (g *gen) lines(ctx context.Context, lg *logrus.Logger, merchant uuid.UUID, products, transactions []uuid.UUID, amount int) error {
	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "transaction_lines")
	if err != nil {
		return fmt.Errorf("failed to establish appender for transaction lines: %w", err)
	}
	defer appender.Close()
	defer lg.WithField("quantity", amount).Info("flushing transaction lines disk")

	for i := 0; i < amount; i++ {
		if err := appender.AppendRow(
			duckdb.UUID(transactions[rand.Int()%len(transactions)]),
			duckdb.UUID(products[rand.Int()%len(products)]),
			int32(rand.Int()%13),
			duckdb.UUID(merchant),
		); err != nil {
			return fmt.Errorf("failed to append transaction line row: %w", err)
		}
		g.stats.lines++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_GENERATED_TRANSACTION_LINES, g.stats.lines)
	}

	return nil
}
