-- +goose Up
-- +goose StatementBegin
CREATE TABLE records (
       id INTEGER PRIMARY KEY,
       created_at INTEGER NOT NULL
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE records;
-- +goose StatementEnd
