CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    description TEXT NOT NULL,
    metadata_json TEXT,
    account_name TEXT NOT NULL,
    payment_id TEXT,       -- Nullable, if event is specific to one payment
    batch_id TEXT,         -- Nullable, if event is specific to a batch

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_events_account ON events(account_name);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_batch_id ON events(batch_id);
CREATE INDEX idx_events_payment_id ON events(payment_id);
