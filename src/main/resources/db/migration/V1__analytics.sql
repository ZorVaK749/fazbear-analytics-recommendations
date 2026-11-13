CREATE TABLE event_inbox (
    event_id uuid PRIMARY KEY,
    topic varchar(80) NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE product_projection (
    product_id bigint PRIMARY KEY CHECK (product_id > 0),
    name varchar(250) NOT NULL,
    category varchar(150) NOT NULL,
    price numeric(19,4) NOT NULL CHECK (price >= 0),
    stock integer NOT NULL CHECK (stock >= 0),
    active boolean NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0)
);
CREATE TABLE interaction (
    event_id uuid PRIMARY KEY REFERENCES event_inbox(event_id),
    tenant_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    product_id bigint NOT NULL CHECK (product_id > 0),
    action varchar(10) NOT NULL CHECK (action IN ('VIEW','SELECT','ADD','UPDATE','REMOVE')),
    quantity integer,
    occurred_at timestamptz NOT NULL,
    topic varchar(80) NOT NULL
);
CREATE INDEX interaction_owner ON interaction(tenant_id, owner_id, occurred_at DESC);
CREATE INDEX interaction_affinity ON interaction(tenant_id, product_id, owner_id);
CREATE TABLE event_quarantine (
    topic varchar(80) NOT NULL,
    partition_id integer NOT NULL,
    record_offset bigint NOT NULL,
    reason varchar(40) NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition_id, record_offset)
);
