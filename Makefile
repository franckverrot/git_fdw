MODULES = git_fdw
MODULE_big = git_fdw

SHLIB_LINK = -lgit2
EXTENSION = git_fdw
OBJS = git_fdw.o
DATA = git_fdw--1.1.0.sql
PGFILEDESC = "git_fdw - foreign data wrapper for git repositories"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
