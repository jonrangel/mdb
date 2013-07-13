all: mdbdump

WARNINGS = -Wall -Werror
OPTS = -O0 -ggdb
FILES = mdb.c mdb.h main.c
PKGS = libbson-1.0

mdbdump: $(FILES)
	$(CC) -o $@ $(WARNINGS) $(OPTS) $(FILES) $(shell pkg-config --cflags --libs $(PKGS))

clean:
	rm -f mdbdump
