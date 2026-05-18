-- +goose Up
-- +goose StatementBegin
CREATE TABLE outbox
(
    id             UUID PRIMARY KEY,
    aggregate_type text        NOT NULL,
    aggregate_id   UUID        NOT NULL,
    topic          text        NOT NULL,
    key            UUID        NOT NULL,
    payload        JSONB       NOT NULL,
    headers        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ DEFAULT NULL
);

CREATE TABLE inbox
(
    PRIMARY KEY (consumer, message_id),
    message_id   UUID        NOT NULL,
    consumer     text        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE outbox;
DROP TABLE inbox;
DROP INDEX idx_outbox_unpublished;
-- +goose StatementEnd
