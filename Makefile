MODULE_big   = auto_index
OBJS         = auto_index.o

EXTENSION    = auto_index
DATA         = auto_index--1.0.sql

PG_CONFIG    = $(HOME)/pg-dev/bin/pg_config
PGXS         := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)