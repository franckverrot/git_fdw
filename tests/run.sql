SHOW server_version_num;
SELECT
  name,
  message,
  sha1
FROM
  repository
WHERE
  sha1 like '4fc2faf9%'
