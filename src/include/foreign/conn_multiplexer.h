/*-------------------------------------------------------------------------
 *
 * conn_multiplexer.h
 *		Connection multiplexer for foreign data wrappers
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/foreign/conn_multiplexer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONN_MULTIPLEXER_H
#define CONN_MULTIPLEXER_H

#include "fmgr.h"

/* Function declarations */
extern void InitConnMultiplexer(void);
extern void RegisterConnMultiplexerWorkers(void);
extern Size conn_multiplexer_shmem_size(void);
extern bool IsConnMultiplexerEnabled(void);
extern int GetNextMultiplexerWorker(void);
extern bool MultiplexerConnect(const char *conninfo, int *conn_id_out);
extern bool MultiplexerQuery(int conn_id, const char *query, void **result_out);
extern void MultiplexerClose(int conn_id);
extern void conn_multiplexer_worker_main(Datum main_arg);

/* SQL-callable progress reporting function */
extern Datum pg_stat_conn_multiplexer(PG_FUNCTION_ARGS);

#endif							/* CONN_MULTIPLEXER_H */
