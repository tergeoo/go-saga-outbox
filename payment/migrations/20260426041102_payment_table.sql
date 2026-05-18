-- +goose Up
-- +goose StatementBegin
CREATE TABLE payment
(
    id           UUID PRIMARY KEY NOT NULL,
    saga_id      UUID UNIQUE      NOT NULL,
    user_id      UUID             NOT NULL,
    amount_cents BIGINT           NOT NULL,
    status       TEXT             NOT NULL,
    external_id  TEXT
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE payment;
-- +goose StatementEnd
