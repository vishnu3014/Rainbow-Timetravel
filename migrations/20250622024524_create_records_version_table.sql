-- +goose Up
-- +goose StatementBegin
create table record_versions (
id integer primary key autoincrement,
version integer not null,
attributes_payload text not null default '{}' check(json_valid(attributes_payload)),
attributes text not null default '{}' check(json_valid(attributes)),
attributes_updated_at integer not null,
record_id integer not null,
created_at integer not null,
foreign key(record_id) references records(id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table record_versions;
-- +goose StatementEnd
