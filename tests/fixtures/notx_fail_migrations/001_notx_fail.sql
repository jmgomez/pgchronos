-- migrate:no-transaction
CREATE TABLE IF NOT EXISTS notx_fail_test (id int);
INSERT INTO notx_fail_test (nonexistent_col) VALUES (1);
