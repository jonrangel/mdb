all: mdbdump mdbundo

WARNINGS = -Wall -Werror
OPTS = -O0 -ggdb
FILES = mdb.c mdb.h
PKGS = libbson-1.0

mdbdump: $(FILES) mdbdump.c
	$(CC) -o $@ $(WARNINGS) $(OPTS) $(FILES) $(shell pkg-config --cflags --libs $(PKGS)) mdbdump.c

mdbundo: $(FILES) mdbundo.c
	$(CC) -o $@ $(WARNINGS) $(OPTS) $(shell pkg-config --cflags --libs $(PKGS)) $(FILES) mdbundo.c

clean:
	rm -f mdbdump mdbundo
