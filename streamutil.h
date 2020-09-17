/*-------------------------------------------------------------------------
 *
 * streamutil.h
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.h
 *-------------------------------------------------------------------------
 */

#ifndef STREAMUTIL_H
#define STREAMUTIL_H

#include "libpq-fe.h"

#include "access/xlogdefs.h"

extern const char *progname;
extern char *connection_string;
extern char *dbhost;
extern char *dbuser;
extern char *dbport;
extern char *dbname;
extern int	dbgetpassword;
extern char *replication_slot;

/* Connection kept global so we can disconnect easily */
struct Connection{
    PGconn *conn;
    Connection(bool replication){
        conn = GetConnection(bool replication);
    }
    /*
 * Create a replication slot for the given connection. This function
 * returns true in case of success as well as the start position
 * obtained after the slot creation.
 */
    bool CreateReplicationSlot(PGconn *conn, const char *slot_name,
                               const char *plugin, bool is_physical,
                               bool slot_exists_ok);

    /*
 * Drop a replication slot for the given connection. This function
 * returns true in case of success.
 */
    bool DropReplicationSlot(const char *slot_name);

/*
 * Run IDENTIFY_SYSTEM through a given connection and give back to caller
 * some result information if requested:
 * - System identifier
 * - Current timeline ID
 * - Start LSN position
 * - Database name (NULL in servers prior to 9.4)
 */
    bool RunIdentifySystem(char **sysid,
                           TimeLineID *starttli,
                           XLogRecPtr *startpos,
                           char **db_name);

};


extern PGconn * GetConnection(bool replication);

/* Replication commands */



extern int64 feGetCurrentTimestamp(void);
extern void feTimestampDifference(int64 start_time, int64 stop_time,
                                  long *secs, int *microsecs);

extern bool feTimestampDifferenceExceeds(int64 start_time, int64 stop_time,
                                         int msec);
extern void fe_sendint64(int64 i, char *buf);
extern int64 fe_recvint64(char *buf);

#endif   /* STREAMUTIL_H */
