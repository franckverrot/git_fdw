CREATE EXTENSION IF NOT EXISTS git_fdw;
CREATE SERVER IF NOT EXISTS git_fdw FOREIGN DATA WRAPPER git_fdw;
CREATE SCHEMA IF NOT EXISTS master;
IMPORT FOREIGN SCHEMA git_data
FROM SERVER git_fdw 
INTO master
OPTIONS (
    path '/home/shackle/pggit/postgresql/.git',
    branch 'refs/heads/master'
);
SELECT * FROM master.repository;
