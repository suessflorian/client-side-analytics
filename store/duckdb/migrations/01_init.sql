CREATE TABLE IF NOT EXISTS main.merchants (
  id UUID, -- PRIMARY KEY
  name VARCHAR
);

CREATE TABLE IF NOT EXISTS main.products (
  id UUID, -- PRIMARY KEY
  name VARCHAR,
  price_cents INTEGER,
  merchant_id UUID, -- REFERENCES main.merchants(id)
);

CREATE TABLE IF NOT EXISTS main.transactions (
  id UUID, -- PRIMARY KEY
  created_at TIMESTAMP,
  merchant_id UUID, -- REFERENCES main.merchants(id)
);

CREATE TABLE IF NOT EXISTS main.transaction_lines (
  id UUID, -- PRIMARY KEY,
  transaction_id UUID, -- REFERENCES main.transactions(id)
  product_id UUID, -- REFERENCES main.products(id)
  quantity INTEGER,
  merchant_id UUID, -- REFERENCES main.merchants(id)
);
