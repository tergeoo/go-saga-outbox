-- +goose Up
-- +goose StatementBegin
CREATE TABLE dead_message
(
    id          UUID PRIMARY KEY,
    message_id  UUID        NOT NULL,
    saga_id     UUID,
    topic       TEXT        NOT NULL,
    payload     BYTEA,
    headers     JSONB       NOT NULL,
    reason      TEXT        NOT NULL,
    consumer    TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    replayed_at TIMESTAMPTZ,
    UNIQUE (consumer, message_id)
);

CREATE INDEX idx_dead_message_saga_id ON dead_message (saga_id);
CREATE INDEX idx_dead_message_unreplayed ON dead_message (created_at) WHERE replayed_at IS NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS dead_message
-- +goose StatementEnd
