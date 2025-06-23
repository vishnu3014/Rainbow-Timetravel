-- +goose Up
-- +goose StatementBegin
create table record_versions (
id integer primary key autoincrement,
attributes text not null default '{}' check(json_valid(attributes)),
actual_update_timestamp integer not null,
record_id integer not null,
created_at integer not null,
foreign key(record_id) references records(id)
);

create index idx_record_versions_timestamp on record_versions(actual_update_timestamp);

create index idx_record_versions_record_id on record_versions(record_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table record_versions;
-- +goose StatementEnd
