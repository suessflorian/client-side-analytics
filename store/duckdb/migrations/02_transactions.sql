CREATE TABLE IF NOT EXISTS main.transactions (
  id UUID PRIMARY KEY,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.transaction_lines (
  transaction_id UUID,
  product_id UUID,
  quantity INTEGER
);
