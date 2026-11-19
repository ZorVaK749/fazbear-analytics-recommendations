CREATE TABLE analytics_archive_pending (
    event_id uuid PRIMARY KEY REFERENCES interaction(event_id),
    next_attempt_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analytics_archive_due ON analytics_archive_pending(next_attempt_at, event_id);

CREATE TABLE dashboard_ticket (
    digest char(64) PRIMARY KEY,
    tenant_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    expires_at timestamptz NOT NULL,
    session_until timestamptz NOT NULL
);
CREATE INDEX dashboard_ticket_expiry ON dashboard_ticket(expires_at);
