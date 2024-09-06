package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"

	"embed"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
)

const (
	where = "duck.db"
)

//go:embed migrations/*.sql
var migrations embed.FS

func Init(ctx context.Context, lg *logrus.Logger, path string) (*duckdb.Connector, error) {
	connector, err := duckdb.NewConnector(where, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb connector: %v", err)
	}

	return migrate(ctx, lg, connector)
}

func migrate(ctx context.Context, lg *logrus.Logger, conn *duckdb.Connector) (*duckdb.Connector, error) {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("failed to list migration files: %v", err)
	}

	sort.Strings(files)

	quantity, last := 0, "none"
	for _, file := range files {
		content, err := migrations.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %v", file, err)
		}

		if _, err := sql.OpenDB(conn).ExecContext(ctx, string(content)); err != nil {
			return nil, fmt.Errorf("failed to apply migration %s: %v", file, err)
		}

		quantity++
		last = file
	}

	lg.WithFields(logrus.Fields{
		"migration": logrus.Fields{
			"quantity": quantity,
			"last":     last,
		},
	}).Info("duck migrations applied")

	return conn, nil
}
