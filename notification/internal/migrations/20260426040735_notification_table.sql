-- +goose Up
-- +goose StatementBegin
CREATE TABLE notification
(
    id      UUID PRIMARY KEY,
    saga_id UUID UNIQUE NOT NULL,
    channel TEXT        NOT NULL,
    status  TEXT        NOT NULL,
    sent_at TIMESTAMPTZ
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE notification;
-- +goose StatementEnd
