-- migrate:no-transaction
CREATE TABLE IF NOT EXISTS notx_test (id int);
CREATE INDEX CONCURRENTLY IF NOT EXISTS notx_test_idx ON notx_test (id);
