#!/usr/bin/env bash
source $(dirname $0)/helpers.sh

# Create and start services
pg_ctlcluster $1 my_cluster start

# Clone git_fdw's repo
git clone https://github.com/franckverrot/git_fdw.git /git_fdw/repo

# Setup Postgres
exec_psql /git_fdw/tests/setup.sql
exec_psql /git_fdw/tests/run.sql

# Execute tests
expected_output=$(cat tests/expected_outputs/$1)
actual_output=$(su -c "$PSQL -txqAF, -R\; < /git_fdw/tests/run.sql" - postgres)

# Assert results
if [ "$expected_output" == "$actual_output" ]
then
  echo "OK"
  exit 0
else
  echo "KO, expected:"
  echo "$expected_output"
  echo "got:"
  echo "$actual_output"
  exit 1
fi
