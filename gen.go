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
	DIAGNOSTIC_TOTAL_TRANSACTIONS      = "Total transactions"
	DIAGNOSTIC_TOTAL_TRANSACTION_LINES = "Total transaction lines"
	DIAGNOSTIC_TOTAL_PRODUCTS          = "Total products"
	DIAGNOSTIC_TOTAL_MERCHANTS         = "Total merchants"
)

type gen struct {
	// overall keeps track of how many different entities exist overall.
	overall   generated
	connector *duckdb.Connector
}

type generated struct {
	Merchants    int
	Products     int
	Transactions int
	Lines        int
}

func newGenerator(ctx context.Context, connector *duckdb.Connector) (*gen, error) {
	g := &gen{connector: connector}

	for table, count := range map[string]*int{
		"merchants":         &g.overall.Merchants,
		"products":          &g.overall.Products,
		"transactions":      &g.overall.Transactions,
		"transaction_lines": &g.overall.Lines,
	} {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		if err := sql.OpenDB(g.connector).QueryRowContext(ctx, query).Scan(count); err != nil {
			return nil, fmt.Errorf("failed to get row count for table %s: %w", table, err)
		}
	}

	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_MERCHANTS, g.overall.Merchants)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_PRODUCTS, g.overall.Products)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_TRANSACTIONS, g.overall.Transactions)
	diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_TRANSACTION_LINES, g.overall.Lines)

	return g, nil
}

func (g *gen) create(ctx context.Context, lg *logrus.Logger, amount int) (generated, error) {
	lg.Info("generator running")

	merchants, err := g.merchants(ctx, lg, amount)
	if err != nil {
		return generated{}, err
	}

	var res = generated{
		Merchants: len(merchants),
	}

	for _, merchant := range merchants {
		products, err := g.products(ctx, lg, merchant, rand.Int()%100)
		if err != nil {
			return res, err
		}
		res.Products += len(products)

		transactions, err := g.transactions(ctx, lg, merchant, rand.Int()%10_000)
		if err != nil {
			return res, err
		}
		res.Transactions += len(transactions)

		lines, err := g.lines(ctx, lg, merchant, products, transactions, len(transactions)*7)
		if err != nil {
			return res, err
		}
		res.Lines += len(lines)
	}

	lg.Info("generator idle")
	return res, err
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
		g.overall.Merchants++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_MERCHANTS, g.overall.Merchants)
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
		products[i] = uuid.Must(uuid.NewV7())
		if err := appender.AppendRow(
			duckdb.UUID(products[i]),
			names[rand.Int()%len(names)]+" "+names[rand.Int()%len(names)],
			rand.Int31()%10_000+100,
			duckdb.UUID(merchant),
		); err != nil {
			return nil, fmt.Errorf("failed to append product row: %w", err)
		}
		g.overall.Products++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_PRODUCTS, g.overall.Products)
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
		transactions[i] = uuid.Must(uuid.NewV7())
		if err := appender.AppendRow(
			duckdb.UUID(transactions[i]),
			now.Add(-time.Duration(rand.Int())*time.Hour),
			duckdb.UUID(merchant),
		); err != nil {
			return nil, fmt.Errorf("failed to append transaction row: %w", err)
		}
		g.overall.Transactions++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_TRANSACTIONS, g.overall.Transactions)
	}

	return transactions, nil
}

func (g *gen) lines(ctx context.Context, lg *logrus.Logger, merchant uuid.UUID, products, transactions []uuid.UUID, amount int) ([]uuid.UUID, error) {
	if len(products) == 0 || len(transactions) == 0 {
		return nil, nil
	}

	conn, err := g.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not connect: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "transaction_lines")
	if err != nil {
		return nil, fmt.Errorf("failed to establish appender for transaction lines: %w", err)
	}
	defer appender.Close()
	defer lg.WithField("quantity", amount).Info("flushing transaction lines disk")

	lines := make([]uuid.UUID, amount)
	for i := 0; i < amount; i++ {
		lines[i] = uuid.Must(uuid.NewV7())
		if err := appender.AppendRow(
			duckdb.UUID(lines[i]),
			duckdb.UUID(transactions[rand.Int()%len(transactions)]),
			duckdb.UUID(products[rand.Int()%len(products)]),
			int32(rand.Int()%13),
			duckdb.UUID(merchant),
		); err != nil {
			return nil, fmt.Errorf("failed to append transaction line row: %w", err)
		}
		g.overall.Lines++
		diagnostics.DiagnosticsFromContext(ctx).Set(DIAGNOSTIC_TOTAL_TRANSACTION_LINES, g.overall.Lines)
	}

	return lines, nil
}
