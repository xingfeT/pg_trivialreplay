/*-------------------------------------------------------------------------
 *
 * pg_trivialreplay.c - trivial logical replication replayer. Mostly based on
 *                      forking pg_recvlogical.
 *
 * assumes decoder_raw from https://github.com/michaelpq/pg_plugins/tree/master/decoder_raw
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>

/* local includes */
#include "streamutil.h"

#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"


/* Time to sleep between reconnection attempts */
#define RECONNECT_SLEEP_TIME 5

/* Global Options */
static int	verbose = 0;
static int	noloop = 0;
static int	standby_message_timeout = 10 * 1000;		/* 10 sec = default */
static int	fsync_interval = 10 * 1000; /* 10 sec = default */
static XLogRecPtr startpos = InvalidXLogRecPtr;
static bool do_create_slot = false;
static bool slot_exists_ok = false;
static bool do_start_slot = false;
static bool do_drop_slot = false;
static bool do_check = false;
static char *log_dsn = NULL;
static char *target_dsn = NULL;

/* filled pairwise with option, value. value may be NULL */
static char **options;
static size_t noptions = 0;
static const char *plugin = "decoder_raw";

/* Global State */
static volatile sig_atomic_t time_to_abort = false;
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;
static XLogRecPtr output_fsync_lsn = InvalidXLogRecPtr;

static void usage(void);
static void StreamLogicalLog(void);
static void disconnect_and_exit(int code);

static void
usage(void)
{
	printf(_("%s receives and replays PostgreSQL logical decoding stream.\n\n"),
		   progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nAction to be performed:\n"));
	printf(_("      --create-slot      create a new replication slot (for the slot's name see --slot)\n"));
	printf(_("      --drop-slot        drop the replication slot (for the slot's name see --slot)\n"));
	printf(_("      --start            start streaming in a replication slot (for the slot's name see --slot)\n"));
	printf(_("      --check            check replica identity of origin tables\n")),
	printf(_("\nOptions:\n"));
	printf(_("      --if-not-exists    do not error if slot already exists when creating a slot\n"));
	printf(_("  -I, --startpos=LSN     where in an existing slot should the streaming start\n"));
	printf(_("  -l, --logserver=DSN    DSN to database to connect and write logs to\n"));
	printf(_("  -n, --no-loop          do not loop on connection lost\n"));
	printf(_("  -o, --option=NAME[=VALUE]\n"
			 "                         pass option NAME with optional value VALUE to the\n"
			 "                         output plugin\n"));
	printf(_("  -P, --plugin=PLUGIN    use output plugin PLUGIN (default: %s)\n"), plugin);
	printf(_("  -s, --status-interval=SECS\n"
			 "                         time between status packets sent to server (default: %d)\n"), (standby_message_timeout / 1000));
	printf(_("  -S, --slot=SLOTNAME    name of the logical replication slot\n"));
	printf(_("  -t, --target-dsn=DSN   DSN to the target database to push changes into\n"));
	printf(_("  -v, --verbose          output verbose messages\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -d, --dbname=DBNAME    database to connect to\n"));
	printf(_("  -h, --host=HOSTNAME    database server host or socket directory\n"));
	printf(_("  -p, --port=PORT        database server port number\n"));
	printf(_("  -U, --username=NAME    connect as specified database user\n"));
	printf(_("  -w, --no-password      never prompt for password\n"));
	printf(_("  -W, --password         force password prompt (should happen automatically)\n"));
	printf(_("\nReport bugs to <pgsql-bugs@postgresql.org>.\n"));
}

/*
 * Send a Standby Status Update message to server.
 */
static bool
sendFeedback(PGconn *conn, int64 now, bool force, bool replyRequested)
{
	static XLogRecPtr last_written_lsn = InvalidXLogRecPtr;
	static XLogRecPtr last_fsync_lsn = InvalidXLogRecPtr;

	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	/*
	 * we normally don't want to send superfluous feedbacks, but if it's
	 * because of a timeout we need to, otherwise wal_sender_timeout will kill
	 * us.
	 */
	if (!force &&
		last_written_lsn == output_written_lsn &&
		last_fsync_lsn != output_fsync_lsn)
		return true;

	if (verbose)
		fprintf(stderr,
		   _("%s: confirming write up to %X/%X, flush to %X/%X (slot %s)\n"),
				progname,
			(uint32) (output_written_lsn >> 32), (uint32) output_written_lsn,
				(uint32) (output_fsync_lsn >> 32), (uint32) output_fsync_lsn,
				replication_slot);

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(output_written_lsn, &replybuf[len]);	/* write */
	len += 8;
	fe_sendint64(output_fsync_lsn, &replybuf[len]);		/* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0;		/* replyRequested */
	len += 1;

	startpos = output_written_lsn;
	last_written_lsn = output_written_lsn;
	last_fsync_lsn = output_fsync_lsn;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		fprintf(stderr, _("%s: could not send feedback packet: %s"),
				progname, PQerrorMessage(conn));
		return false;
	}

	return true;
}

static void
disconnect_and_exit(int code)
{
	if (conn != NULL)
		PQfinish(conn);

	exit(code);
}

static bool
prepare_logserver(PGconn *log_conn)
{
	PGresult *res;

	res = PQexec(log_conn, "CREATE TABLE IF NOT EXISTS trivialreplay_log (ts timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, logpos int8, sql text)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not create log table\n"), progname);
		PQclear(res);
		return false;
	}
	PQclear(res);

	res = PQprepare(log_conn, "log_ins",
					"INSERT INTO trivialreplay_log (logpos, sql) VALUES ($1, $2)",
					2,
					(int[]){20, 25});
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not prepare logging query\n"), progname);
		PQclear(res);
		return false;
	}
	PQclear(res);

	return true;
}

static bool
log_write(PGconn *log_conn, XLogRecPtr lsn, char *sql)
{
	PGresult *res;

	if (log_conn)
	{
		uint64_t lsn_out = htobe64(lsn);
		res = PQexecPrepared(log_conn,
							 "log_ins",
							 2,
							 (const char *[]){(char *)&lsn_out, sql},
							 (int[]){sizeof(lsn_out), 0},
							 (int[]){1, 0},
							 0);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			fprintf(stderr, _("%s: could not write to log: %s\n"), progname,  PQerrorMessage(log_conn));
			PQclear(res);
			return false;
		}
		PQclear(res);
	}
	return true;
}

static bool
prepare_targetserver(PGconn *target_conn)
{
	PGresult   *res;

	res = PQexec(target_conn, "CREATE TABLE IF NOT EXISTS _logicalreceive.status (commit_pos int8)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not create status table\n"), progname);
		PQclear(res);
		return false;
	}
	PQclear(res);

	res = PQexec(target_conn, "INSERT INTO _logicalreceive.status SELECT 0 WHERE NOT EXISTS (SELECT * FROM _logicalreceive.status)");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not populate status table\n"), progname);
		PQclear(res);
		return false;
	}
	PQclear(res);

	res = PQprepare(target_conn, "upd", "UPDATE _logicalreceive.status SET commit_pos=$1", 1, (int[]){20});
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not prepare location update query\n"), progname);
		PQclear(res);
		return false;
	}

	return true;
}

/*
 * Start the log streaming
 */
static void
StreamLogicalLog(void)
{
	PGresult   *res;
	char	   *copybuf = NULL;
	int64		last_status = -1;
	int			i;
	PQExpBuffer query;
	PGconn	   *log_conn = NULL;
	PGconn	   *target_conn = NULL;
	bool		in_transaction = false;

	output_written_lsn = InvalidXLogRecPtr;
	output_fsync_lsn = InvalidXLogRecPtr;

	query = createPQExpBuffer();

	/*
	 * Connect in replication mode to the server
	 */
	if (!conn)
		conn = GetConnection(true);
	if (!conn)
		/* Error message already written in GetConnection() */
		return;

	if (log_dsn)
	{
		log_conn = PQconnectdb(log_dsn);
		if (!log_dsn || PQstatus(log_conn) != CONNECTION_OK)
		{
			fprintf(stderr, _("%s: could not connect to log server: %s"), progname, PQerrorMessage(log_conn));
			goto error;
		}
	}
	if (log_dsn && !prepare_logserver(log_conn))
		goto error;

	target_conn = PQconnectdb(target_dsn);
	if (!target_conn || PQstatus(target_conn) != CONNECTION_OK)
	{
		fprintf(stderr, _("%s: could not connect to target server: %s"), progname, PQerrorMessage(target_conn));
		goto error;
	}

	/* Disable triggers on the receiving end */
	res = PQexec(target_conn, "SET session_replication_role='replica'");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, _("%s: could not set session replication role"), progname);
		PQclear(res);
		goto error;
	}

	/* Prepare statements and metadata tables */
	if (!prepare_targetserver(target_conn))
		goto error;

	res = PQexec(target_conn, "SELECT commit_pos FROM _logicalreceive.status");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not get start position\n"), progname);
		PQclear(res);
		goto error;
	}
	startpos = atoll(PQgetvalue(res, 0, 0));
	PQclear(res);


	/*
	 * Start the replication
	 */
	if (verbose)
		fprintf(stderr,
				_("%s: starting log streaming at %X/%X (slot %s)\n"),
				progname, (uint32) (startpos >> 32), (uint32) startpos,
				replication_slot);

	/* Initiate the replication stream at specified location */
	appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
			 replication_slot, (uint32) (startpos >> 32), (uint32) startpos);

	/* print options if there are any */
	if (noptions)
		appendPQExpBufferStr(query, " (");

	for (i = 0; i < noptions; i++)
	{
		/* separator */
		if (i > 0)
			appendPQExpBufferStr(query, ", ");

		/* write option name */
		appendPQExpBuffer(query, "\"%s\"", options[(i * 2)]);

		/* write option value if specified */
		if (options[(i * 2) + 1] != NULL)
			appendPQExpBuffer(query, " '%s'", options[(i * 2) + 1]);
	}

	if (noptions)
		appendPQExpBufferChar(query, ')');

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		fprintf(stderr, _("%s: could not send replication command \"%s\": %s"),
				progname, query->data, PQresultErrorMessage(res));
		PQclear(res);
		goto error;
	}
	PQclear(res);
	resetPQExpBuffer(query);

	if (verbose)
		fprintf(stderr,
				_("%s: streaming initiated\n"),
				progname);

	while (!time_to_abort)
	{
		int			r;
		int			bytes_left;
		int			bytes_written;
		int64		now;
		int			hdr_len;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		/*
		 * Potentially send a status message to the master
		 */
		now = feGetCurrentTimestamp();

		if (standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(last_status, now,
										 standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, now, true, false))
				goto error;

			last_status = now;
		}

		r = PQgetCopyData(conn, &copybuf, 1);
		if (r == 0)
		{
			/*
			 * In async mode, and no data available. We block on reading but
			 * not more than the specified timeout, so that we can send a
			 * response back to the client.
			 */
			fd_set		input_mask;
			int64		message_target = 0;
			int64		fsync_target = 0;
			struct timeval timeout;
			struct timeval *timeoutptr = NULL;

			FD_ZERO(&input_mask);
			FD_SET(PQsocket(conn), &input_mask);

			/* Compute when we need to wakeup to send a keepalive message. */
			if (standby_message_timeout)
				message_target = last_status + (standby_message_timeout - 1) *
					((int64) 1000);

			/* Now compute when to wakeup. */
			if (message_target > 0 || fsync_target > 0)
			{
				int64		targettime;
				long		secs;
				int			usecs;

				targettime = message_target;

				if (fsync_target > 0 && fsync_target < targettime)
					targettime = fsync_target;

				feTimestampDifference(now,
									  targettime,
									  &secs,
									  &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;
			}

			r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				fprintf(stderr, _("%s: select() failed: %s\n"),
						progname, strerror(errno));
				goto error;
			}

			/* Else there is actually data on the socket */
			if (PQconsumeInput(conn) == 0)
			{
				fprintf(stderr,
						_("%s: could not receive data from WAL stream: %s"),
						progname, PQerrorMessage(conn));
				goto error;
			}
			continue;
		}

		/* End of copy stream */
		if (r == -1)
			break;

		/* Failure while reading the copy stream */
		if (r == -2)
		{
			fprintf(stderr, _("%s: could not read COPY data: %s"),
					progname, PQerrorMessage(conn));
			goto error;
		}

		/* Check the message type. */
		if (copybuf[0] == 'k')
		{
			int			pos;
			bool		replyRequested;
			XLogRecPtr	walEnd;

			/*
			 * Parse the keepalive message, enclosed in the CopyData message.
			 * We just check if the server requested a reply, and ignore the
			 * rest.
			 */
			pos = 1;			/* skip msgtype 'k' */
			walEnd = fe_recvint64(&copybuf[pos]);
			output_written_lsn = Max(walEnd, output_written_lsn);

			pos += 8;			/* read walEnd */

			pos += 8;			/* skip sendTime */

			if (r < pos + 1)
			{
				fprintf(stderr, _("%s: streaming header too small: %d\n"),
						progname, r);
				goto error;
			}
			replyRequested = copybuf[pos];

			/* If the server requested an immediate reply, send one. */
			if (replyRequested)
			{
				now = feGetCurrentTimestamp();
				if (!sendFeedback(conn, now, true, false))
					goto error;
				last_status = now;
			}
			continue;
		}
		else if (copybuf[0] != 'w')
		{
			fprintf(stderr, _("%s: unrecognized streaming header: \"%c\"\n"),
					progname, copybuf[0]);
			goto error;
		}


		/*
		 * Read the header of the XLogData message, enclosed in the CopyData
		 * message. We only need the WAL location field (dataStart), the rest
		 * of the header is ignored.
		 */
		hdr_len = 1;			/* msgtype 'w' */
		hdr_len += 8;			/* dataStart */
		hdr_len += 8;			/* walEnd */
		hdr_len += 8;			/* sendTime */
		if (r < hdr_len + 1)
		{
			fprintf(stderr, _("%s: streaming header too small: %d\n"),
					progname, r);
			goto error;
		}

		/* Extract WAL location for this block */
		{
			XLogRecPtr	temp = fe_recvint64(&copybuf[1]);

			output_written_lsn = Max(temp, output_written_lsn);
		}

		bytes_left = r - hdr_len;

		/*
		 * Copy promises that the returned string is always null terminated,
		 * and it will be the last thing here, so we can print it.
		 */
		if (strcmp(copybuf+hdr_len, "BEGIN;") == 0)
		{
			if (in_transaction)
			{
				fprintf(stderr, _("%s: received BEGIN when already in transaction.\n"), progname);
				goto error;
			}
			else
			{
				/* Open a transaction. */
				res = PQexec(target_conn, "BEGIN");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, _("%s: could not open transaction on target: %s\n"), progname, PQerrorMessage(target_conn));
					PQclear(res);
					goto error;
				}
				in_transaction = true;

				if (!log_write(log_conn, output_written_lsn, "BEGIN"))
					goto error;
			}
		}
		else if (strcmp(copybuf+hdr_len, "COMMIT;") == 0)
		{
			if (!in_transaction)
			{
				fprintf(stderr, _("%s: received COMMIT when not in a transaction.\n"), progname);
				goto error;
			}
			else
			{
				uint64_t lsn_out = htobe64(output_written_lsn);

				/* Commit the transaction, including updating where we are */
				res = PQexecPrepared(target_conn, "upd", 1, (const char *[]){(char *)&lsn_out},
									 (int[]){sizeof(lsn_out)}, (int[]){1}, 0);
				if (PQresultStatus(res) != PGRES_COMMAND_OK || atoi(PQcmdTuples(res)) != 1)
				{
					fprintf(stderr, _("%s: could not update status counter: %s\n"), progname, PQerrorMessage(target_conn));
					PQclear(res);
					goto error;
				}

				/* Now that we're here, commit it */
				res = PQexec(target_conn, "COMMIT");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, _("%s: could not commit transaction: %s\n"), progname, PQerrorMessage(target_conn));
					PQclear(res);
				}
				in_transaction = false;

				if (!log_write(log_conn, output_written_lsn, "COMMIT"))
					goto error;

				/* Finally tell the server that we're happy */
				output_fsync_lsn = output_written_lsn;
				sendFeedback(conn, now, true, false);
			}
		}
		else
		{
			res = PQexec(target_conn, copybuf+hdr_len);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, _("%s: could not replay: %s\n"), progname, PQerrorMessage(target_conn));
				PQclear(res);
				goto error;
			}
			PQclear(res);

			if (!log_write(log_conn, output_written_lsn, copybuf+hdr_len))
				goto error;

		}
	}

	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr,
				_("%s: unexpected termination of replication stream: %s"),
				progname, PQresultErrorMessage(res));
		goto error;
	}
	PQclear(res);

error:
	if (copybuf != NULL)
	{
		PQfreemem(copybuf);
		copybuf = NULL;
	}
	destroyPQExpBuffer(query);
	PQfinish(conn);
	conn = NULL;
	if (target_conn)
	{
		PQfinish(target_conn);
		target_conn = NULL;
	}
	if (log_conn)
	{
		PQfinish(log_conn);
		log_conn = NULL;
	}
}

static void
check_origin(void)
{
	PGresult *res;
	int i;

	conn = GetConnection(false);
	if (!conn)
		/* Error message already printed */
		return;

	res = PQexec(conn, "SELECT quote_ident(nspname), quote_ident(relname) FROM pg_class INNER JOIN pg_namespace ON pg_namespace.oid=relnamespace WHERE relkind='r' AND NOT (nspname LIKE 'pg_%' OR nspname='information_schema') AND (relreplident='n' OR (relreplident='d' AND NOT relhaspkey))");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, _("%s: could not query for tables: %s\n"),
				progname, PQerrorMessage(conn));
		return;
	}

	if (PQntuples(res) == 0)
	{
		printf("All tables can be replicated.\n");
		PQclear(res);
		return;
	}

	for (i = 0; i < PQntuples(res); i++)
	{
		printf("Table %s.%s cannot replicate UPDATE/DELETE due to lack of REPLICA IDENTITY\n",
			   PQgetvalue(res, i, 0),
			   PQgetvalue(res, i, 1));
	}
	PQclear(res);
}

/*
 * Unfortunately we can't do sensible signal handling on windows...
 */
#ifndef WIN32

/*
 * When sigint is called, just tell the system to exit at the next possible
 * moment.
 */
static void
sigint_handler(int signum)
{
	time_to_abort = true;
}

#endif

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
/* general options */
		{"file", required_argument, NULL, 'f'},
		{"fsync-interval", required_argument, NULL, 'F'},
		{"no-loop", no_argument, NULL, 'n'},
		{"verbose", no_argument, NULL, 'v'},
		{"version", no_argument, NULL, 'V'},
		{"help", no_argument, NULL, '?'},
/* connection options */
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
/* replication options */
		{"startpos", required_argument, NULL, 'I'},
		{"option", required_argument, NULL, 'o'},
		{"plugin", required_argument, NULL, 'P'},
		{"status-interval", required_argument, NULL, 's'},
		{"slot", required_argument, NULL, 'S'},
		{"target-dsn", required_argument, NULL, 't'},
/* action */
		{"create-slot", no_argument, NULL, 1},
		{"start", no_argument, NULL, 2},
		{"drop-slot", no_argument, NULL, 3},
		{"if-not-exists", no_argument, NULL, 4},
		{"check", no_argument, NULL, 5},
		{NULL, 0, NULL, 0}
	};
	int			c;
	int			option_index;
	uint32		hi,
				lo;
	char	   *db_name;

	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_basebackup"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		else if (strcmp(argv[1], "-V") == 0 ||
				 strcmp(argv[1], "--version") == 0)
		{
			puts("pg_recvlogical (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "f:F:nvd:h:p:U:wWI:o:P:s:S:l:t:",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
/* general options */
			case 'n':
				noloop = 1;
				break;
			case 'v':
				verbose++;
				break;
/* connection options */
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'h':
				dbhost = pg_strdup(optarg);
				break;
			case 'p':
				if (atoi(optarg) <= 0)
				{
					fprintf(stderr, _("%s: invalid port number \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				dbport = pg_strdup(optarg);
				break;
			case 'U':
				dbuser = pg_strdup(optarg);
				break;
			case 'w':
				dbgetpassword = -1;
				break;
			case 'W':
				dbgetpassword = 1;
				break;
/* replication options */
			case 'I':
				if (sscanf(optarg, "%X/%X", &hi, &lo) != 2)
				{
					fprintf(stderr,
							_("%s: could not parse start position \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				startpos = ((uint64) hi) << 32 | lo;
				break;
			case 'o':
				{
					char	   *data = pg_strdup(optarg);
					char	   *val = strchr(data, '=');

					if (val != NULL)
					{
						/* remove =; separate data from val */
						*val = '\0';
						val++;
					}

					noptions += 1;
					options = pg_realloc(options, sizeof(char *) * noptions * 2);

					options[(noptions - 1) * 2] = data;
					options[(noptions - 1) * 2 + 1] = val;
				}

				break;
			case 'P':
				plugin = pg_strdup(optarg);
				break;
			case 's':
				standby_message_timeout = atoi(optarg) * 1000;
				if (standby_message_timeout < 0)
				{
					fprintf(stderr, _("%s: invalid status interval \"%s\"\n"),
							progname, optarg);
					exit(1);
				}
				break;
			case 'S':
				replication_slot = pg_strdup(optarg);
				break;
			case 'l':
				log_dsn = pg_strdup(optarg);
				break;
			case 't':
				target_dsn = pg_strdup(optarg);
				break;
/* action */
			case 1:
				do_create_slot = true;
				break;
			case 2:
				do_start_slot = true;
				break;
			case 3:
				do_drop_slot = true;
				break;
			case 4:
				slot_exists_ok = true;
				break;
			case 5:
				do_check = true;
				break;

			default:

				/*
				 * getopt_long already emitted a complaint
				 */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	/*
	 * Any non-option arguments?
	 */
	if (optind < argc)
	{
		fprintf(stderr,
				_("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	/*
	 * Required arguments
	 */
	if (replication_slot == NULL && !do_check)
	{
		fprintf(stderr, _("%s: no slot specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (target_dsn == NULL && do_start_slot)
	{
		fprintf(stderr, _("%s: no target DSN specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (!do_drop_slot && dbname == NULL)
	{
		fprintf(stderr, _("%s: no database specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (!do_drop_slot && !do_create_slot && !do_start_slot && !do_check)
	{
		fprintf(stderr, _("%s: at least one action needs to be specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (do_check && (do_create_slot || do_start_slot || do_drop_slot))
	{
		fprintf(stderr, _("%s: cannot use --check together with any other actions\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (do_drop_slot && (do_create_slot || do_start_slot))
	{
		fprintf(stderr, _("%s: cannot use --create-slot or --start together with --drop-slot\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (startpos != InvalidXLogRecPtr && (do_create_slot || do_drop_slot))
	{
		fprintf(stderr, _("%s: cannot use --create-slot or --drop-slot together with --startpos\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

#ifndef WIN32
	pqsignal(SIGINT, sigint_handler);
#endif

	if (do_check)
	{
		/* do_check doesn't use the standard connection, so call it separtely */
		check_origin();
		disconnect_and_exit(0);
	}

	/*
	 * Obtain a connection to server. This is not really necessary but it
	 * helps to get more precise error messages about authentification,
	 * required GUC parameters and such.
	 */
	conn = GetConnection(true);
	if (!conn)
		/* Error message already written in GetConnection() */
		exit(1);

	/*
	 * Run IDENTIFY_SYSTEM to make sure we connected using a database specific
	 * replication connection.
	 */
	if (!RunIdentifySystem(conn, NULL, NULL, NULL, &db_name))
		disconnect_and_exit(1);

	if (db_name == NULL)
	{
		fprintf(stderr,
				_("%s: could not establish database-specific replication connection\n"),
				progname);
		disconnect_and_exit(1);
	}

	/* Drop a replication slot. */
	if (do_drop_slot)
	{
		if (verbose)
			fprintf(stderr,
					_("%s: dropping replication slot \"%s\"\n"),
					progname, replication_slot);

		if (!DropReplicationSlot(conn, replication_slot))
			disconnect_and_exit(1);
	}

	/* Create a replication slot. */
	if (do_create_slot)
	{
		if (verbose)
			fprintf(stderr,
					_("%s: creating replication slot \"%s\"\n"),
					progname, replication_slot);

		if (!CreateReplicationSlot(conn, replication_slot, plugin,
								   false, slot_exists_ok))
			disconnect_and_exit(1);
		startpos = InvalidXLogRecPtr;
	}

	if (!do_start_slot)
		disconnect_and_exit(0);

	/* Stream loop */
	while (true)
	{
		StreamLogicalLog();
		if (time_to_abort)
		{
			/*
			 * We've been Ctrl-C'ed. That's not an error, so exit without an
			 * errorcode.
			 */
			disconnect_and_exit(0);
		}
		else if (noloop)
		{
			fprintf(stderr, _("%s: disconnected\n"), progname);
			exit(1);
		}
		else
		{
			fprintf(stderr,
			/* translator: check source for value for %d */
					_("%s: disconnected; waiting %d seconds to try again\n"),
					progname, RECONNECT_SLEEP_TIME);
			pg_usleep(RECONNECT_SLEEP_TIME * 1000000);
		}
	}
}
