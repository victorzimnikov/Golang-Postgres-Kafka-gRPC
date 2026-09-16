CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  event_version INTEGER NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,

  CONSTRAINT outbox_events_event_version_positive
    CHECK (event_version > 0),

  CONSTRAINT outbox_events_attempts_not_negative
    CHECK (attempts >= 0)
);

CREATE INDEX outbox_events_unpublished_idx
  ON outbox_events (occurred_at, id)
  WHERE published_at IS NULL;