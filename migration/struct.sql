CREATE TABLE IF NOT EXISTS entity_table
(
    ent_id       text PRIMARY KEY,
    data         float     NOT NULL,
    value        float     NOT NULL,
    date_added   timestamp NOT NULL DEFAULT NOW(),
    date_updated timestamp
);

