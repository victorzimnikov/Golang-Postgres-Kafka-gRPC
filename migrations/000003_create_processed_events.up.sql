CREATE TABLE processed_events (
  consumer_group TEXT NOT NULL,
  event_id UUID NOT NULL,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  offset_value BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT processed_events_partition_not_negative
    CHECK (partition >= 0),

  CONSTRAINT processed_events_offset_value_not_negative
    CHECK (offset_value >= 0),

  CONSTRAINT processed_events_pkey
    PRIMARY KEY (consumer_group, event_id),

  CONSTRAINT processed_events_offset_unique
    UNIQUE (consumer_group, topic, partition, offset_value)
);
