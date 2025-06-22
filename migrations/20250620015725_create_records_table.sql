-- +goose Up
-- +goose StatementBegin
CREATE TABLE records (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE records;
-- +goose StatementEnd
