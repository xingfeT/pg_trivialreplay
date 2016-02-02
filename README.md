# pg_trivialreplay

`pg_trivialreplay` is a trivial fork of `pg_recvlogical` from PostgreSQL 9.5
that instead of writing the data out to a file, will write it directly to
a target PostgreSQL database. As such it forms a very trivial receiving end
of a logical replication solution. It is intended to be very lightweight,
and does *not* represent a complete solution for logical replication! The
main motivation for it's existance is the ability to replicate into old versions
of PostgreSQL, not supported by the normal logical replication solutions.

`pg_trivialreplay` normally uses the `decoder_raw` plugin on the server side,
which generates the SQL statements to be written. These statements will be
directly run on the target server, so if a different plugin is used, strange
errors will likely occur.

Finally, a log of all operations will be written to a `separate` database.
This could be a separate database on the same cluster as the receiving end,
but should normally be a completely separate database.

### DDL

`pg_trivialreplay` does not attempt any DDL replication, and does not make
any verifications that table schemas match. If there are any mismatches in
schema, it will error on apply and give up.

## Installing

To build, `pg_config` from PostgreSQL 9.5 needs to be available in the PATH.
Once that is present, just running `make` should build the frontend. No
installation is necessary as it's just a single binary, which can be copied.

`pg_trivialreplay` can run either on the receiving server or the sending
server.

### decoder_raw

To work, `decoder_raw` needs to be installed on the *upstream 9.5 server*.
`decoder_raw` is found in Michael Paquiers package `pg_plugins` at
https://github.com/michaelpq/pg_plugins/. The rest of the package is not
necessary, so only the `decoder_raw` subdirectory needs a `make install`.

## Running

### Permissions

`pg_trivialreplay` can be run on either the receiving or originating end
of the replication, or on a completely separate node. It requires replication
permissions on the origin node, and full read/write permissions for the user
tables on the target system.

On the target system, the schema `_logicalreceive` must exist and
`pg_trivialreplay` needs permissions to create a table in it.

If logging is used, the table `trivialreplay_log` will be created in the
database that's targeted.

### Running

`pg_trivialreplay` will do it's own restarting if it gets an error on the
connection, based on how `pg_recvlogical` works, but it's probably a good
idea to run it in a restarting wrapper anyway. it takes most parameters
similar to `pg_recvlogical`, and also:

> -l \<dsn\>

  DSN for log connection. If this is not specified, logging will not be done. If
  it is specified, a connection will be made to it and all SQL statements will
  be sent there as well.

> -t \<dsn\>

  DSN for target database. This is the database that all the statements will be
  replayed.

As with normal logical replication, first create a slot, and then use it. It
is **very** important to always set `include_transaction=on` as an option!

#### Example

```
$ pg_trivialreplay --create \
  -d postgres -S trivialslot

$ pg_trivialreplay --start \
  -l "port=5432 dbname=postgres" \
  -t "port=5432 dbname=receiver" \
  -d postgres -S trivialslot -o include_transaction=on\
  -v
```