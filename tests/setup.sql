CREATE EXTENSION git_fdw;
CREATE SERVER git_fdw_server
  FOREIGN DATA WRAPPER git_fdw;
CREATE FOREIGN TABLE
  repository (
      sha1          text,
      message       text,
      name          text,
      email         text,
      commit_date   timestamp with time zone,
      insertions    int,
      deletions     int,
      files_changed int
  )
SERVER git_fdw_server
OPTIONS (
  path '/git_fdw/repo/.git',
  branch 'master'
);
