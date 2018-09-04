CFLAGS=-I$(shell pg_config --includedir-server) -I$(shell pg_config --includedir) -I$(shell pg_config --includedir)/internal

LDFLAGS=-Wall -L$(shell pg_config --pkglibdir) -L../inst/head/lib -lpq -L../inst/head/lib -lpgcommon -L../inst/head/lib -lpgport

OBJS=pg_trivialreplay.o streamutil.o receivelog.o

pg_trivialreplay: ${OBJS}
	${CC} ${OBJS} ${LDFLAGS} -o pg_trivialreplay

clean:
	rm -f ${OBJS}
	rm -f pg_trivialreplay
