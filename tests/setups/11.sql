IMPORT FOREIGN SCHEMA git_data
  FROM SERVER git_fdw_server
  INTO git_repos
  OPTIONS (
    path   '/git_fdw/repo.git',
    branch 'refs/heads/master',
    prefix 'rails_',
    git_search_path '/optional/custom/search_path'
  );
