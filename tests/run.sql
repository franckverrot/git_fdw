SHOW server_version_num;
SELECT
  name,
  message,
  sha1,
  insertions,
  deletions,
  files_changed
FROM
  git_repos.rails_repository
WHERE
  sha1 like '4fc2faf9%'
