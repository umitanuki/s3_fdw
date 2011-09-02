#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "curl/curl.h"

#include "postgres.h"
#include "fmgr.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "postmaster/fork_process.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/rel.h"
//#include "utils/resowner.h"

#include "connutil.h"

PG_MODULE_MAGIC;


/*
 * Describes the valid options for objects that use this wrapper.
 */
struct S3FdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for s3_fdw.
 * These options are based on the options for COPY FROM command.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * s3GetOptions(), which currently doesn't bother to look at user mappings.
 */
static struct S3FdwOption valid_options[] = {
	/* File options */
	{"filename", ForeignTableRelationId},

	{"bucketname", ForeignTableRelationId},
	{"hostname", ForeignTableRelationId},

	/* Format options */
	/* oids option is not supported */
	{"format", ForeignTableRelationId},
	{"header", ForeignTableRelationId},
	{"delimiter", ForeignTableRelationId},
	{"quote", ForeignTableRelationId},
	{"escape", ForeignTableRelationId},
	{"null", ForeignTableRelationId},
	{"encoding", ForeignTableRelationId},

	{"accesskey", UserMappingRelationId},
	{"secretkey", UserMappingRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct S3FdwExecutionState
{
	char	   *hostname;
	char	   *bucketname;
	char	   *filename;		/* file to read */
	char	   *accesskey;
	char	   *secretkey;
	List	   *copy_options;	/* merged COPY options, excluding filename */
	CopyState	cstate;			/* state of reading file */
	char	   *datafn;
} S3FdwExecutionState;

/*
 * forked processes communicate via FIFO, which is described
 * in this struct. Some experiments tell that it should be
 * a bad idead to re-open these FIFO; we prepare two files
 * as one for synchronizing flag, the other for data transfer.
 */
typedef struct s3_ipc_context
{
	char		datafn[MAXPGPATH];
	FILE	   *datafp;
	char		flagfn[MAXPGPATH];
	FILE	   *flagfp;
} s3_ipc_context;

/*
 * Function declarations
 */
PG_FUNCTION_INFO_V1(s3test);
PG_FUNCTION_INFO_V1(s3_fdw_handler);
PG_FUNCTION_INFO_V1(s3_fdw_validator);

Datum s3test(PG_FUNCTION_ARGS);
Datum s3_fdw_handler(PG_FUNCTION_ARGS);
Datum s3_fdw_validator(PG_FUNCTION_ARGS);

/*
 * FDW callback routines
 */
static FdwPlan *s3PlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel);
static void s3ExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void s3BeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *s3IterateForeignScan(ForeignScanState *node);
static void s3ReScanForeignScan(ForeignScanState *node);
static void s3EndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void s3GetOptions(Oid foreigntableid, S3FdwExecutionState *state);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   const char *filename,
			   Cost *startup_cost, Cost *total_cost);

static size_t header_handler(void *buffer, size_t size, size_t nmemb, void *buf);
static size_t body_handler(void *buffer, size_t size, size_t nmemb, void *userp);
static size_t write_data_to_buf(void *buffer, size_t size, size_t nmemb, void *buf);

static char *create_tempprefix(char *seed);
//static void s3_resource_release(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);
static void s3_on_exit(int code, Datum arg);

void _PG_init(void);

Datum
s3test(PG_FUNCTION_ARGS)
{
	CURL		   *curl;
	StringInfoData	buf;
	int				sc;
	char		   *url;
	char			tmp[1024];
	struct curl_slist *slist;
	char		   *datestring;
	char		   *signature;

	char		   *bucket = "umitanuki-dbtest";
	char		   *file = "1.txt";
	char		   *host = "s3-ap-northeast-1.amazonaws.com";

	char		   *accesskey = "";
	char		   *secretkey = "";

	url = text_to_cstring(PG_GETARG_TEXT_P(0));

	url = palloc0(1024);
	snprintf(url, 1024, "http://%s/%s/%s", host, bucket, file);
	datestring = httpdate(NULL);
	signature = s3_signature("GET", datestring, bucket, file, secretkey);

	slist = NULL;
	snprintf(tmp, sizeof(tmp), "Date: %s", datestring);
	slist = curl_slist_append(slist, tmp);
	snprintf(tmp, sizeof(tmp), "Authorization: AWS %s:%s", accesskey, signature);
	slist = curl_slist_append(slist, tmp);
	initStringInfo(&buf);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data_to_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	sc = curl_easy_perform(curl);

	curl_easy_cleanup(curl);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
s3_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->PlanForeignScan = s3PlanForeignScan;
	fdwroutine->ExplainForeignScan = s3ExplainForeignScan;
	fdwroutine->BeginForeignScan = s3BeginForeignScan;
	fdwroutine->IterateForeignScan = s3IterateForeignScan;
	fdwroutine->ReScanForeignScan = s3ReScanForeignScan;
	fdwroutine->EndForeignScan = s3EndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
s3_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *filename = NULL,
			   *bucketname = NULL,
			   *hostname = NULL,
			   *accesskey = NULL,
			   *secretkey = NULL;
	List	   *copy_options = NIL;
	ListCell   *cell;

	/*
	 * Check that only options supported by s3_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			struct S3FdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.data)));
		}

		/* Separate out filename, since ProcessCopyOptions won't allow it */
		if (strcmp(def->defname, "filename") == 0)
		{
			if (filename)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			filename = defGetString(def);
		}
		else if(strcmp(def->defname, "bucketname") == 0)
		{
			if (bucketname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			bucketname = defGetString(def);
		}
		else if(strcmp(def->defname, "hostname") == 0)
		{
			if (hostname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			hostname = defGetString(def);
		}
		else if(strcmp(def->defname, "accesskey") == 0)
		{	
			if (accesskey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			accesskey = defGetString(def);
		}
		else if(strcmp(def->defname, "secretkey") == 0)
		{
			if (secretkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			secretkey = defGetString(def);
		}
		else
			copy_options = lappend(copy_options, def);
	}

	/*
	 * Now apply the core COPY code's validation logic for more checks.
	 */
	ProcessCopyOptions(NULL, true, copy_options);

	/*
	 * Hostname option is required for s3_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && hostname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("hostname is required for s3_fdw foreign tables")));

	/*
	 * Bucketname option is required for s3_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && bucketname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("bucketname is required for s3_fdw foreign tables")));

	/*
	 * Filename option is required for s3_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && filename == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("filename is required for s3_fdw foreign tables")));

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	struct S3FdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a s3_fdw foreign table.
 *
 * We have to separate out "filename", "bucketname" and "hostname"
 * from the other options because it must not appear in the options
 * list passed to the core COPY code.
 */
static void
s3GetOptions(Oid foreigntableid, S3FdwExecutionState *state)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	UserMapping  *mapping;
	List	   *options, *new_options;
	ListCell   *lc;

	/*
	 * Extract options from FDW objects.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	mapping = GetUserMapping(GetUserId(), table->serverid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);
	options = list_concat(options, table->options);

	/*
	 * Separate out the host, bucket and filename.
	 */
	state->hostname = NULL;
	state->bucketname = NULL;
	state->filename = NULL;
	new_options = NIL;
	foreach(lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "hostname") == 0)
		{
			state->hostname = defGetString(def);
		}
		else if (strcmp(def->defname, "bucketname") == 0)
		{
			state->bucketname = defGetString(def);
		}
		else if (strcmp(def->defname, "filename") == 0)
		{
			state->filename = defGetString(def);
		}
		else if (strcmp(def->defname, "accesskey") == 0)
		{
			state->accesskey = defGetString(def);
		}
		else if (strcmp(def->defname, "secretkey") == 0)
		{
			state->secretkey = defGetString(def);
		}
		else
			new_options = lappend(new_options, def);
	}

	/*
	 * The validator should have checked those mandatory options were
	 * included in the options, but check again, just in case.
	 */
	if (state->hostname == NULL)
		elog(ERROR, "hostname is required for s3_fdw foreign tables");
	if (state->bucketname == NULL)
		elog(ERROR, "bucketname is required for s3_fdw foreign tables");
	if (state->filename == NULL)
		elog(ERROR, "filename is required for s3_fdw foreign tables");

	state->copy_options = new_options;
}

/*
 * s3PlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
s3PlanForeignScan(Oid foreigntableid,
				  PlannerInfo *root,
				  RelOptInfo *baserel)
{
	FdwPlan		    *fdwplan;
	S3FdwExecutionState state;

	/* Fetch options -- it's not sure what is needed here */
	s3GetOptions(foreigntableid, &state);

	/* Construct FdwPlan with cost estimates */
	fdwplan = makeNode(FdwPlan);
	estimate_costs(root, baserel, state.filename,
				   &fdwplan->startup_cost, &fdwplan->total_cost);
	fdwplan->fdw_private = NIL; /* not used */

	return fdwplan;
}

/*
 * s3ExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
s3ExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	S3FdwExecutionState	state;
	StringInfoData		url;

	initStringInfo(&url);

	/* Fetch options */
	s3GetOptions(RelationGetRelid(node->ss.ss_currentRelation), &state);
	appendStringInfo(&url, "http://%s/%s/%s",
					 state.hostname, state.bucketname, state.filename);

	ExplainPropertyText("Foreign URL", url.data, es);
}

/*
 * s3BeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
s3BeginForeignScan(ForeignScanState *node, int eflags)
{
	CopyState	cstate;
	S3FdwExecutionState *festate;
	StringInfoData buf;
	char	   *url, *datestring, *signature;
	char	   *prefix;
	pid_t		pid;
	s3_ipc_context ctx;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	festate = (S3FdwExecutionState *) palloc(sizeof(S3FdwExecutionState));
	/* Fetch options of foreign table */
	s3GetOptions(RelationGetRelid(node->ss.ss_currentRelation), festate);

	initStringInfo(&buf);
	appendStringInfo(&buf, "http://%s/%s/%s",
					 festate->hostname, festate->bucketname, festate->filename);
	url = pstrdup(buf.data);

	datestring = httpdate(NULL);
	signature = s3_signature("GET", datestring,
							 festate->bucketname, festate->filename, festate->secretkey);

	prefix = create_tempprefix(signature);
	snprintf(ctx.flagfn, sizeof(ctx.flagfn), "%s.flag", prefix);
	snprintf(ctx.datafn, sizeof(ctx.datafn), "%s.data", prefix);
//	unlink(ctx.flagfn);
//	unlink(ctx.datafn);
	if (mkfifo(ctx.flagfn, S_IRUSR | S_IWUSR) != 0)
		elog(ERROR, "mkfifo failed(%d):%s", errno, ctx.flagfn);
	if (mkfifo(ctx.datafn, S_IRUSR | S_IWUSR) != 0)
		elog(ERROR, "mkfifo failed(%d):%s", errno, ctx.datafn);
	

	/*
	 * Fork to maximize parallelism of input from HTTP and output to SQL.
	 * The spawned child process cheats by on_exit_rest() to die immediately.
	 */
	pid = fork_process();
	if (pid == 0)			/* child */
	{
		struct curl_slist *slist;
		CURL	   *curl;
		int			sc;

		MyProcPid = getpid();	/* reset MyProcPid */

		/*
		 * The exit callback routines clean up
		 * unnecessary resources holded the parent process.
		 * The child dies silently when finishing its job.
		 */
		on_exit_reset();

		/*
		 * Set up request header list
		 */
		slist = NULL;
		initStringInfo(&buf);
		appendStringInfo(&buf, "Date: %s", datestring);
		slist = curl_slist_append(slist, buf.data);
		resetStringInfo(&buf);
		appendStringInfo(&buf, "Authorization: AWS %s:%s",
						 festate->accesskey, signature);
		slist = curl_slist_append(slist, buf.data);
		/*
		 * Set up CURL instance.
		 */
		curl = curl_easy_init();
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_handler);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, &ctx);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, body_handler);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);
		sc = curl_easy_perform(curl);
		if (ctx.datafp)
			FreeFile(ctx.datafp);
		if (sc != 0)
		{
			elog(NOTICE, "%s:curl_easy_perform = %d", url, sc);
			unlink(ctx.datafn);
		}
		curl_slist_free_all(slist);
		curl_easy_cleanup(curl);

		proc_exit(0);
	}
	elog(DEBUG1, "child pid = %d", pid);

	{
		int		status;
		FILE   *fp;

		fp = AllocateFile(ctx.flagfn, PG_BINARY_R);
		read(fileno(fp), &status, sizeof(int));
		FreeFile(fp);
		unlink(ctx.flagfn);
		if (status != 200)
		{
			elog(ERROR, "bad input from API. Status code: %d", status);
		}
	}

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(node->ss.ss_currentRelation,
						   ctx.datafn,
						   NIL,
						   festate->copy_options);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate->cstate = cstate;
	festate->datafn = pstrdup(ctx.datafn);

	node->fdw_state = (void *) festate;
}

/*
 * s3IterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
s3IterateForeignScan(ForeignScanState *node)
{
	S3FdwExecutionState *festate = (S3FdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	bool		found;
	ErrorContextCallback errcontext;

	/* Set up callback to identify error line number. */
	errcontext.callback = CopyFromErrorCallback;
	errcontext.arg = (void *) festate->cstate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);
	found = NextCopyFrom(festate->cstate, NULL,
						 slot->tts_values, slot->tts_isnull,
						 NULL);
	if (found)
		ExecStoreVirtualTuple(slot);

	/* Remove error callback. */
	error_context_stack = errcontext.previous;

	return slot;
}

/*
 * s3EndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
s3EndForeignScan(ForeignScanState *node)
{
	S3FdwExecutionState *festate = (S3FdwExecutionState *) node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		EndCopyFrom(festate->cstate);
		unlink(festate->datafn);
	}
}

/*
 * s3ReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
s3ReScanForeignScan(ForeignScanState *node)
{
	S3FdwExecutionState *festate = (S3FdwExecutionState *) node->fdw_state;

	EndCopyFrom(festate->cstate);

	festate->cstate = BeginCopyFrom(node->ss.ss_currentRelation,
									festate->filename,
									NIL,
									festate->copy_options);
}

/*
 * Estimate costs of scanning a foreign table.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   const char *filename,
			   Cost *startup_cost, Cost *total_cost)
{
//	struct stat stat_buf;
	BlockNumber pages;
//	int			tuple_width;
	double		ntuples;
	double		nrows;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * Get size of the file.  It might not be there at plan time, though, in
	 * which case we have to use a default estimate.
	 */
//	if (stat(filename, &stat_buf) < 0)
//		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate below.
	 */
//	pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
//	if (pages < 1)
//		pages = 1;
	pages = 10;

	/*
	 * Estimate the number of tuples in the file.  We back into this estimate
	 * using the planner's idea of the relation width; which is bogus if not
	 * all columns are being read, not to mention that the text representation
	 * of a row probably isn't the same size as its internal representation.
	 * FIXME later.
	 */
//	tuple_width = MAXALIGN(baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));

//	ntuples = clamp_row_est((double) stat_buf.st_size / (double) tuple_width);
	ntuples = 1000;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.	This is pretty bogus too, since the planner
	 * will have no stats about the relation, but it's better than nothing.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;

	/*
	 * Now estimate costs.	We estimate costs almost the same way as
	 * cost_seqscan(), thus assuming that I/O costs are equivalent to a
	 * regular table file of the same size.  However, we take per-tuple CPU
	 * costs as 10x of a seqscan, to account for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}

static size_t
header_handler(void *buffer, size_t size, size_t nmemb, void *userp)
{
	const char *HTTP_1_1 = "HTTP/1.1";
	size_t		segsize = size * nmemb;
	s3_ipc_context *ctx = (s3_ipc_context *) userp;

	if (strncmp(buffer, HTTP_1_1, strlen(HTTP_1_1)) == 0)
	{
		int		status;

		status = atoi((char *) buffer + strlen(HTTP_1_1) + 1);
		ctx->flagfp = AllocateFile(ctx->flagfn, PG_BINARY_W);
		write(fileno(ctx->flagfp), &status, sizeof(int));
		FreeFile(ctx->flagfp);
		if (status != 200)
		{
			ctx->datafp = NULL;
			/* interrupt */
			return 0;
		}
		/* iif success */
//		ctx->datafp = AllocateFile(ctx->dataname, PG_BINARY_W);
		ctx->datafp = AllocateFile(ctx->datafn, PG_BINARY_W);
	}
	
	return segsize;
}

static size_t
body_handler(void *buffer, size_t size, size_t nmemb, void *userp)
{
	size_t		segsize = size * nmemb;
	s3_ipc_context *ctx = (s3_ipc_context *) userp;

	fwrite(buffer, size, nmemb, ctx->datafp);

	return segsize;
}

static size_t
write_data_to_buf(void *buffer, size_t size, size_t nmemb, void *userp)
{
	size_t		segsize = size * nmemb;
	StringInfo	info = (StringInfo) userp;

	appendBinaryStringInfo(info, (const char *) buffer, segsize);

	return segsize;
}

static char *
create_tempprefix(char *seed)
{
	char	filename[MAXPGPATH], path[MAXPGPATH], *s;

	snprintf(filename, sizeof(filename), "%u.%s", MyProcPid, seed);
	s = &filename[0];
	while(*s)
	{
		if (*s == '/')
			*s = '%';
		s++;
	}
	mkdir("base/" PG_TEMP_FILES_DIR, S_IRWXU);
	snprintf(path, sizeof(path), "base/%s/%s", PG_TEMP_FILES_DIR, filename);

	return pstrdup(path);
}

static bool
presuffix_test(char *base, char *prefix, char *suffix)
{
	int		len, plen, slen;

	len = strlen(base);
	plen = strlen(prefix);
	slen = strlen(suffix);
	if (len < plen + slen)
		return false;

	return memcmp(base, prefix, plen) == 0 &&
		memcmp(base + len - slen, suffix, slen) == 0;
}

/*
 * Clean up fifos on process exit.
 * We don't care other process's fifo since it may be
 * in use right now. It might be better to delete fifos
 * as soone as possible, but they don't consume disk space
 * so let's postpone it till exit, where it's sure to delete.
 */
static void
s3_on_exit(int code, Datum arg)
{
	char	prefix[32];
	char   *dirname = "base/" PG_TEMP_FILES_DIR;
	DIR	   *dir;
	struct dirent *ent;

	snprintf(prefix, sizeof(prefix), "%d.", MyProcPid);
	dir = AllocateDir(dirname);
	while((ent = ReadDir(dir, dirname)) != NULL)
	{
		int		len;

		len = strlen(ent->d_name);
		if (presuffix_test(ent->d_name, prefix, ".data") ||
			presuffix_test(ent->d_name, prefix, ".flag"))
		{
			unlink(ent->d_name);
		}
	}

	FreeDir(dir);
}

void
_PG_init(void)
{
	on_proc_exit(s3_on_exit, (Datum) 0);
//	RegisterResourceReleaseCallback(s3_resource_release, NULL);
}
