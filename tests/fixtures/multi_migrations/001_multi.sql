CREATE TABLE multi_test (id int PRIMARY KEY);
CREATE OR REPLACE FUNCTION multi_test_noop() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;
CREATE INDEX multi_test_idx ON multi_test (id);
