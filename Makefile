
MODULE_big = s3_fdw
OBJS = s3_fdw.o connutil.o# copy_patched.o
EXTENSION = $(MODULE_big)
EXTVERSION = 0.1.0
EXTSQL = $(MODULE_big)--$(EXTVERSION).sql
DATA = $(EXTSQL)
EXTRA_CLEAN += $(EXTSQL)
SHLIB_LINK = -lcurl -lssl -lcrypto

#DOCS = doc/$(MODULES).md
REGRESS = $(MODULE_big)

all: $(EXTSQL)

$(EXTSQL): $(MODULE_big).sql
	cp $< $@



PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
