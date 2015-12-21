# PostgreSQL Git Foreign Data Wrapper

git\_fdw is a a Git Foreign Data Wrapper for PostgreSQL written in C.

It is making use of [`libgit2`](libgit2.github.com).

## INSTALLATION

### Prerequisites

* libgit2-dev


### Setup

Building git\_fdw is as simple as

    make

and installing it only requires oneself to

    make install


Now you can start setting up your environment to access git repositories:

    λ psql
    psql (9.4.1)
    Type "help" for help.

    franck=# CREATE EXTENSION git_fdw;
    CREATE EXTENSION

    franck=# CREATE SERVER git_fdw_server FOREIGN DATA WRAPPER git_fdw;
    CREATE SERVER

    franck=# CREATE FOREIGN TABLE
        rails_repository (
            sha1        text,
            message     text,
            name        text,
            email       text,
            commit_date timestamp with time zone
        )
        SERVER git_fdw_server
        OPTIONS (
            path   '/home/franck/rails/.git',
            branch 'master'
        );
    CREATE FOREIGN TABLE

    franck=# SELECT message, name FROM rails_repository LIMIT 10;
                             message                          |          name
    ----------------------------------------------------------+------------------------
     Revert "Merge pull request #15312 from JuanitoFatas/acti | Matthew Draper
     Merge pull request #16908 from y-yagi/change_activejob_t | Abdelkader Boudih
     Change ActiveJob test directory to "test/jobs"          +| yuuji.yaginuma
                                                              |
     Merge pull request #16669 from aantix/dangerous_attribut | Rafael Mendonça França
     Changed the DangerousAttributeError exception message to | Jim Jones
     Prepare maintenance policy for 4.2 release [ci skip]    +| Rafael Mendonça França
                                                              |
     Se the test order of activejob tests                    +| Rafael Mendonça França
                                                              |
     Change gid calls to to_gid                              +| Rafael Mendonça França
                                                              |
     Merge pull request #16897 from kostia/message-varifier-r | Rafael Mendonça França
     Changes "if secret.nil?" to unless secret in MessageVerf | Kostiantyn Kahanskyi
    (10 rows)


It is not possible to access multiple repositories through the same foreign
table. We suggest the usage of views if this is something that needs to be
achieved.

## CONFIGURATION

### Server

There are no options that can be passed to a git\_fdw server.

### Foreign Table

The possible options are:

* `path`: The path of the git repository;
* `branch`: The branch to be used.

## TODO

* [ ] Publish the extension on [PGXN](http://pgxn.org/)


## Note on Patches/Pull Requests

* Fork the project.
* Make your feature addition or bug fix.
* Add tests for it. This is important so I don't break it in a future version unintentionally.
* Commit, do not mess with version or history. (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull)
* Send me a pull request. Bonus points for topic branches.

## LICENSE

Copyright (c) 2014-2015 Franck Verrot. MIT LICENSE. See LICENSE.md for details.
