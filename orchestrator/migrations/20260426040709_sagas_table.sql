-- +goose Up
-- +goose StatementBegin
CREATE TABLE saga
(
    id              UUID PRIMARY KEY NOT NULL,
    type            TEXT             NOT NULL,
    state           TEXT             NOT NULL,
    current_step    TEXT             NOT NULL,
    payload         JSONB            NOT NULL,
    context         JSONB,
    attempts        INT              NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ      NOT NULL,
    updated_at      TIMESTAMPTZ      NOT NULL
);

CREATE TABLE saga_step
(
    id                 UUID PRIMARY KEY NOT NULL,
    saga_id            UUID             NOT NULL,
    step_name          TEXT             NOT NULL,
    direction          TEXT             NOT NULL,
    status             TEXT             NOT NULL,
    command_message_id UUID,
    reply_message_id   UUID,
    error              TEXT,
    created_at         TIMESTAMPTZ      NOT NULL
);

CREATE INDEX idx_state_next_attempt_at on saga (state, next_attempt_at);
CREATE INDEX idx_id_created_at on saga (id, created_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE saga;
DROP TABLE saga_step;
DROP INDEX idx_state_next_attempt_at;
DROP INDEX idx_id_created_at;
-- +goose StatementEnd
