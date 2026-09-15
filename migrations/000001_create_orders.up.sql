CREATE TABLE orders (
  id UUID PRIMARY KEY,
  customer_id TEXT NOT NULL,
  amount_kopecks BIGINT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,

  CONSTRAINT orders_customer_id_not_empty
    CHECK (customer_id <> ''),

  CONSTRAINT orders_amount_positive
    CHECK (amount_kopecks > 0),

  CONSTRAINT orders_status_valid
    CHECK (status IN ('pending'))
);