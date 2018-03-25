CREATE FOREIGN TABLE
  git_repos.rails_repository (
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
    path '/git_fdw/repo.git',
    branch 'refs/heads/master',
    git_search_path '/optional/custom/search_path'
  );
