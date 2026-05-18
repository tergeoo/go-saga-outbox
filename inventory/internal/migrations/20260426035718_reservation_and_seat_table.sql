-- +goose Up
-- +goose StatementBegin
CREATE TABLE reservation
(
    id         UUID PRIMARY KEY NOT NULL,
    saga_id    UUID             NOT NULL UNIQUE,
    seat_id    UUID             NOT NULL,
    status     text             NOT NULL,
    created_at TIMESTAMPTZ      NOT NULL
);

CREATE TABLE seat
(
    id       UUID PRIMARY KEY NOT NULL,
    event_id UUID             NOT NULL,
    status   text             NOT NULL
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE reservation;
DROP TABLE seat;
-- +goose StatementEnd
