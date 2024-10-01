package main

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
)

type analytics struct {
	connector *duckdb.Connector
}

type ProductRevenue struct {
	ProductID    uuid.UUID `json:"product_id"`
	ProductName  string    `json:"product_name"`
	TotalRevenue float64   `json:"total_revenue"`
}

func (a *analytics) GetTopProducts(ctx context.Context, merchantID uuid.UUID) ([]ProductRevenue, error) {
	query := `
        SELECT 
          p.id AS product_id,
          p.name AS product_name,
          SUM(p.price_cents * tl.quantity) AS total_revenue
        FROM main.products p
        JOIN main.transaction_lines tl ON p.id = tl.product_id
          WHERE tl.merchant_id = ?
        GROUP BY p.id, p.name
        ORDER BY total_revenue DESC, product_name ASC
        LIMIT 5;
    `
	rows, err := sql.OpenDB(a.connector).QueryContext(ctx, query, merchantID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var res []ProductRevenue
	for rows.Next() {
		var product ProductRevenue
		if err := rows.Scan(&product.ProductID, &product.ProductName, &product.TotalRevenue); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		res = append(res, product)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return res, nil
}

func (a *analytics) csvDump(ctx context.Context, w io.Writer, merchantID uuid.UUID) error {
	db := sql.OpenDB(a.connector)

	zip := zip.NewWriter(w)
	defer zip.Close()

	rows, err := db.QueryContext(ctx, `
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'merchant_id'
        GROUP BY table_schema, table_name;
    `)
	if err != nil {
		return fmt.Errorf("failed to query information_schema: %w", err)
	}

	type info struct {
		Schema string
		Name   string
	}
	var tables []info

	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return fmt.Errorf("failed to scan table info: %w", err)
		}
		tables = append(tables, info{Schema: schema, Name: name})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over table list: %w", err)
	}
	rows.Close()

	for _, table := range tables {
		fileName := fmt.Sprintf("%s_%s.csv", table.Schema, table.Name)
		csvFile, err := zip.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create CSV file in ZIP for %s: %w", table.Name, err)
		}

		writer := csv.NewWriter(csvFile)

		rows, err := db.QueryContext(ctx, `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2 AND column_name != 'merchant_id'
            ORDER BY ordinal_position;
        `, table.Schema, table.Name)
		if err != nil {
			return fmt.Errorf("failed to query columns for table %q: %w", table.Name, err)
		}

		var columns []string
		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan column name for table %s: %w", table.Name, err)
			}
			columns = append(columns, colName)
		}
		rows.Close()

		if len(columns) == 0 {
			continue
		}

		if err := writer.Write(columns); err != nil {
			return fmt.Errorf("failed to write column headers for %s: %w", table.Name, err)
		}

		selectQuery := fmt.Sprintf("SELECT * EXCLUDE(merchant_id) FROM %s WHERE merchant_id = ?", table.Name)
		dataRows, err := db.QueryContext(ctx, selectQuery, merchantID)
		if err != nil {
			return fmt.Errorf("failed to query data from table %s: %w", table.Name, err)
		}

		colTypes, err := dataRows.ColumnTypes()
		if err != nil {
			dataRows.Close()
			return fmt.Errorf("failed to get column types for table %s: %w", table.Name, err)
		}

		numCols := len(colTypes)
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for dataRows.Next() {
			if err := dataRows.Scan(valuePtrs...); err != nil {
				dataRows.Close()
				return fmt.Errorf("failed to scan row in table %s: %w", table.Name, err)
			}

			record := make([]string, numCols)
			for i, val := range values {
				if val != nil {
					switch v := val.(type) {
					case []byte:
						if len(v) == 16 {
							u, err := uuid.FromBytes(v)
							if err != nil {
								record[i] = fmt.Sprintf("%x", v)
							} else {
								record[i] = u.String()
							}
						} else {
							record[i] = string(v)
						}
					case time.Time:
						record[i] = v.Format(time.RFC3339Nano)
					default:
						record[i] = fmt.Sprintf("%v", val)
					}
				} else {
					record[i] = ""
				}
			}

			if err := writer.Write(record); err != nil {
				dataRows.Close()
				return fmt.Errorf("failed to write record in table %s: %w", table.Name, err)
			}
		}
		dataRows.Close()

		if err := dataRows.Err(); err != nil {
			return fmt.Errorf("row iteration error for table %s: %w", table.Name, err)
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			return fmt.Errorf("error flushing CSV writer for table %s: %w", table.Name, err)
		}
	}

	return nil
}
