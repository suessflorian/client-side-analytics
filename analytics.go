package main

import (
	"context"
	"database/sql"
	"fmt"

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
