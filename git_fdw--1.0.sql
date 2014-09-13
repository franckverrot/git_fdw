\echo Use "CREATE EXTENSION git_fdw" to load this file. \quit

CREATE FUNCTION git_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION git_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER git_fdw
  HANDLER git_fdw_handler
  VALIDATOR git_fdw_validator;
