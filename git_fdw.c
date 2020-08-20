#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <git2.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

#if (PG_VERSION_NUM < 120000)
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif

#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "plan_state.h"
#include "execution_state.h"
#include "options.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(git_fdw_handler);
PG_FUNCTION_INFO_V1(git_fdw_validator);

#define POSTGRES_TO_UNIX_EPOCH_DAYS (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define POSTGRES_TO_UNIX_EPOCH_USECS (POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)
#define DEFAULT_BRANCH "refs/heads/master"

/* 1 byte for the prefix, another one for the last NULL byte */
#define PADDING (1 + 1)
#define SHA1_LENGTH 40

/* How to Get diff of the first commit?
 * see https://stackoverflow.com/questions/40883798/how-to-get-git-diff-of-the-first-commit
 */

#define EMPTY_REPO_SHA1 "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

typedef enum callback_type
{
  CBT_ERROR,
  CBT_COMMIT
} callback_type_t;

typedef struct callback_obj
{
  callback_type_t type;
  void *data;
} callback_obj_t;

static void gitGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void gitGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *gitGetForeignPlan(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid,
                                      ForeignPath *best_path,
                                      List *tlist,
                                      List *scan_clauses
#if (PG_VERSION_NUM >= 90500)
                                      ,
                                      Plan *outer_plan
#endif
);
static void gitBeginForeignScan(ForeignScanState *node, int eflags);
static void gitExplainForeignScan(ForeignScanState *node, ExplainState *es);
static TupleTableSlot *gitIterateForeignScan(ForeignScanState *node);
static void gitReScanForeignScan(ForeignScanState *node);
static void gitEndForeignScan(ForeignScanState *node);
#if (PG_VERSION_NUM >= 90500)
static List *gitImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif

static bool is_valid_option(const char *option, Oid context);
static void gitGetOptions(Oid foreigntableid, GitFdwPlanState *state, List **other_options);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
                           GitFdwPlanState *fdw_private,
                           Cost *startup_cost, Cost *total_cost);
bool gitAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
int gitAcquireSampleRowsFunc(Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows);
int walkRepository(const char *path,
                   const char *branch,
                   const char *git_search_path,
                   void *callback_state,
                   void (*callback)(void *, callback_obj_t *));
void try_count(void *callback_state, callback_obj_t *obj);
void acquire_sample_rows_callback(void *callback_state, callback_obj_t *obj);
int get_size(GitFdwPlanState *fdw_private);

Datum git_fdw_handler(PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);

  fdwroutine->GetForeignRelSize = gitGetForeignRelSize;
  fdwroutine->GetForeignPaths = gitGetForeignPaths;
  fdwroutine->GetForeignPlan = gitGetForeignPlan;

  fdwroutine->BeginForeignScan = gitBeginForeignScan;
  fdwroutine->IterateForeignScan = gitIterateForeignScan;
  fdwroutine->EndForeignScan = gitEndForeignScan;
  fdwroutine->ExplainForeignScan = gitExplainForeignScan;

  fdwroutine->ReScanForeignScan = gitReScanForeignScan;

  fdwroutine->AnalyzeForeignTable = gitAnalyzeForeignTable;

#if (PG_VERSION_NUM >= 90500)
  /* support for IMPORT FOREIGN SCHEMA */
  fdwroutine->ImportForeignSchema = gitImportForeignSchema;
#endif

  PG_RETURN_POINTER(fdwroutine);
}

Datum git_fdw_validator(PG_FUNCTION_ARGS)
{
  List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
  Oid catalog = PG_GETARG_OID(1);
  char *path = NULL;
  char *branch = NULL;
  char *git_search_path = NULL;
  List *other_options = NIL;
  ListCell *cell;

  foreach (cell, options_list)
  {
    DefElem *def = (DefElem *)lfirst(cell);

    if (!is_valid_option(def->defname, catalog))
    {
      const struct GitFdwOption *opt;
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

      ereport(WARNING,
              (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
               errmsg("invalid option \"%s\"", def->defname),
               buf.len > 0
                   ? errhint("Valid options in this context are: %s",
                             buf.data)
                   : errhint("There are no valid options in this context.")));
    }

    if (strcmp(def->defname, "path") == 0)
    {
      if (path)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("conflicting or redundant options")));
      path = defGetString(def);
    }
    else if (strcmp(def->defname, "branch") == 0)
    {
      if (branch)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("conflicting or redundant options")));
      branch = defGetString(def);
    }
    else if (strcmp(def->defname, "git_search_path") == 0)
    {
      if (git_search_path)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("conflicting or redundant options")));
      git_search_path = defGetString(def);
    }
    else
      other_options = lappend(other_options, def);
  }

  if (catalog == ForeignTableRelationId && path == NULL)
  {
    elog(ERROR, "path is required for git_fdw foreign tables (path of the .git repo)");
  }

  PG_RETURN_VOID();
}

static bool is_valid_option(const char *option, Oid context)
{
  const struct GitFdwOption *opt;

  for (opt = valid_options; opt->optname; opt++)
  {
    if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
      return true;
  }
  return false;
}

// Fetch path and options from the server (only)
static void gitGetOptions(Oid foreigntableid, GitFdwPlanState *state, List **other_options)
{
  ForeignTable *table;
  List *options;
  ListCell *lc;

  table = GetForeignTable(foreigntableid);

  options = NIL;
  options = list_concat(options, table->options);

  foreach (lc, options)
  {
    DefElem *def = (DefElem *)lfirst(lc);

    if (strcmp(def->defname, "path") == 0)
    {
      state->path = defGetString(def);
    }

    if (strcmp(def->defname, "branch") == 0)
    {
      state->branch = defGetString(def);
    }

    if (strcmp(def->defname, "git_search_path") == 0)
    {
      state->git_search_path = defGetString(def);
    }
  }

  if (state->path == NULL)
  {
    elog(ERROR, "path is required for git_fdw foreign tables (path of the .git repo)");
  }

  if (state->branch == NULL)
  {
    state->branch = DEFAULT_BRANCH;
  }

  *other_options = options;
}

typedef struct try_count_walker_state
{
  int rows;
  int dead_rows;
} try_count_walker_state_t;

void try_count(void *callback_state, callback_obj_t *obj)
{
  switch (obj->type)
  {
  case CBT_ERROR:
    ((try_count_walker_state_t *)callback_state)->dead_rows++;
  case CBT_COMMIT:
    ((try_count_walker_state_t *)callback_state)->rows++;
  }
}

static void gitGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
  GitFdwPlanState *fdw_private = (GitFdwPlanState *)palloc(sizeof(GitFdwPlanState));
  gitGetOptions(foreigntableid, fdw_private, &fdw_private->options);

  baserel->fdw_private = (void *)fdw_private;
  baserel->rows = get_size(fdw_private);
}

int get_size(GitFdwPlanState *fdw_private)
{
  try_count_walker_state_t try_count_walker_state = {0, 0};

  walkRepository(fdw_private->path,
                 fdw_private->branch,
                 fdw_private->git_search_path,
                 &try_count_walker_state,
                 try_count);
  return try_count_walker_state.rows;
}

static void gitGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
  GitFdwPlanState *fdw_private = (GitFdwPlanState *)baserel->fdw_private;
  Cost startup_cost;
  Cost total_cost;
  List *coptions = NIL;
  Path *path;

  int size = get_size(fdw_private);
  fdw_private->ntuples = size;
  fdw_private->pages = size;

  /* Estimate costs */
  estimate_costs(root, baserel, fdw_private, &startup_cost, &total_cost);

  /*
   * Create a ForeignPath node and add it as only possible path.  We use the
   * fdw_private list of the path to carry the convert_selectively option;
   * it will be propagated into the fdw_private list of the Plan node.
   */
  path = (Path *)create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
                                         NULL, /* default pathtarget */
#endif
                                         baserel->rows,
                                         startup_cost,
                                         total_cost,
                                         NIL,  /* no pathkeys */
                                         NULL, /* no outer rel either */
#if PG_VERSION_NUM >= 90500
                                         NULL, /* no extra plan */
#endif
                                         coptions);

  add_path(baserel, path);
}

static ForeignScan *
gitGetForeignPlan(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid,
                  ForeignPath *best_path,
                  List *tlist,
                  List *scan_clauses
#if (PG_VERSION_NUM >= 90500)
                  ,
                  Plan *outer_plan
#endif
)
{
  ForeignScan *scan;
  Index scan_relid = baserel->relid;
  scan_clauses = extract_actual_clauses(scan_clauses, false);

  best_path->fdw_private = baserel->fdw_private;

  scan = make_foreignscan(
      tlist,
      scan_clauses,
      scan_relid,
      NIL,
      best_path->fdw_private
#if PG_VERSION_NUM >= 90500
      ,
      NIL, NIL, outer_plan
#endif
  ); /* Not assuming outer_plan is null */

  return scan;
}

static void gitExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
  GitFdwPlanState state;
  List *options;
  Oid relationId = RelationGetRelid(node->ss.ss_currentRelation);

  gitGetOptions(relationId, &state, &options);
  ExplainPropertyText("Foreign Git Repository", state.path, es);
  ExplainPropertyText("Foreign Git Branch", state.branch, es);
  ExplainPropertyText("Foreign Git Search Path", state.git_search_path, es);
}

static void gitBeginForeignScan(ForeignScanState *node, int eflags)
{
  GitFdwExecutionState *festate;
  git_oid oid = {0};
  git_remote *remote = NULL;
  int error;
  const git_remote_head **refs;
  size_t refs_len, i;
  git_remote_callbacks callbacks = GIT_REMOTE_CALLBACKS_INIT;
  Oid relationId = RelationGetRelid(node->ss.ss_currentRelation);
  List *options;
  GitFdwPlanState state;

  git_libgit2_init();
  gitGetOptions(relationId, &state, &options);

  festate = (GitFdwExecutionState *)palloc(sizeof(GitFdwExecutionState));
  festate->path = state.path;
  festate->branch = state.branch;
  festate->git_search_path = state.git_search_path;
  festate->repo = NULL;
  festate->walker = NULL;

  node->fdw_state = (void *)festate;

  if (festate->git_search_path != NULL)
  {
    git_libgit2_opts(
        GIT_OPT_SET_SEARCH_PATH,
        GIT_CONFIG_LEVEL_GLOBAL,
        festate->git_search_path);
  }

  if (festate->repo == NULL)
  {
    int repo_opened = -1;
    if ((repo_opened = git_repository_open(&festate->repo, festate->path)) != GIT_OK)
    {
      const git_error *err = giterr_last();
      ereport(ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg("Failed opening repository: '%s'", festate->path),
               errdetail("libgit2 returned error code %d: %s.", repo_opened, err->message)));
      return;
    }

    error = git_remote_lookup(&remote, festate->repo, festate->path);
    if (error < 0)
    {
      error = git_remote_create_anonymous(&remote, festate->repo, festate->path);
      if (error < 0)
      {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Call to git_remote_create_anonymous failed"),
                        errdetail("Error code: %d", error)));
      }
    }

    error = git_remote_connect(
        remote,
        GIT_DIRECTION_FETCH,
        &callbacks,
        NULL
#if LIBGIT2_VER_MINOR > 24 || LIBGIT2_VER_MAJOR >= 1
        ,
        NULL
#endif
    );
    if (error < 0)
    {
      ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                      errmsg("Call to git_remote_connect failed"),
                      errdetail("Error code: %d", error)));
    }

    error = git_remote_ls(&refs, &refs_len, remote);
    if (error < 0)
    {
      ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                      errmsg("Call to git_remote_ls failed"),
                      errdetail("Error code: %d", error)));
    }

    for (i = 0; i < refs_len; i++)
    {
      if (0 == strcmp(refs[i]->name, festate->branch))
      {
        oid = refs[i]->oid;
        break;
      }
    }

    if (git_oid_iszero(&oid))
    {
      ereport(ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg("Couldn't find branch %s", festate->branch)));
    }

    git_remote_free(remote);
    git_revwalk_new(&(festate->walker), festate->repo);
    git_revwalk_sorting(festate->walker, GIT_SORT_TOPOLOGICAL);
    git_revwalk_push(festate->walker, &oid);
  }
  else
  {
    ereport(WARNING, (errcode(WARNING), errmsg("Repo is already initialized %p", festate->repo), errdetail("No details.")));
  }
}

static TupleTableSlot *gitIterateForeignScan(ForeignScanState *node)
{
  GitFdwExecutionState *festate = (GitFdwExecutionState *)node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

  git_oid oid;
  git_commit *commit;
  git_tree *commit_tree;
  git_commit *commit_parent;
  git_tree *commit_parent_tree;
  git_diff *commit_diff;
  git_diff_stats *commit_diff_stats;
  bool commit_parent_needs_free = false;
  int position;

  /* libgit's output */
  const char *commit_message;
  const git_signature *commit_author;
  const git_oid *commit_sha1;

  /* input data that will get converted to PG data structures */
  char *formatted_commit_id;

  /* PG data structures */
  Datum sha1, message, name, email, date, insertions = 0, deletions = 0, files_changed = 0;

  ExecClearTuple(slot);

  if (git_revwalk_next(&oid, festate->walker) == GIT_OK)
  {
    if (git_commit_lookup(&commit, festate->repo, &oid))
    {
      elog(ERROR, "Failed to lookup the next object\n");
      return NULL;
    }

    commit_message = git_commit_message(commit);
    commit_author = git_commit_committer(commit);
    commit_sha1 = git_commit_id(commit);

    if (git_commit_parent(&commit_parent, commit, 0) == GIT_OK)
    {
      commit_parent_needs_free = true;
      git_commit_tree(&commit_parent_tree, commit_parent);
    }
    else
    {
      /* Get diff of the first commit. */
      git_oid oid_tree_empty;

      if (git_oid_fromstr(&oid_tree_empty, EMPTY_REPO_SHA1) == GIT_OK)
      {
        git_tree_lookup(&commit_parent_tree, festate->repo, &oid_tree_empty);
      }
    }

    if (git_commit_tree(&commit_tree, commit) == GIT_OK &&
        git_diff_tree_to_tree(&commit_diff, festate->repo, commit_parent_tree, commit_tree, NULL) == GIT_OK &&
        git_diff_get_stats(&commit_diff_stats, commit_diff) == GIT_OK)
    {
      insertions = git_diff_stats_insertions(commit_diff_stats);
      deletions = git_diff_stats_deletions(commit_diff_stats);
      files_changed = git_diff_stats_files_changed(commit_diff_stats);

      git_diff_stats_free(commit_diff_stats);
      git_diff_free(commit_diff);

      if (commit_parent_needs_free)
      {
        git_commit_free(commit_parent);
      }
    }
    git_tree_free(commit_tree);
    git_tree_free(commit_parent_tree);

    /* Retrieve string-encoded SHA1 */
    formatted_commit_id = (char *)palloc(SHA1_LENGTH + 1);
    git_oid_fmt(formatted_commit_id, commit_sha1);
    formatted_commit_id[SHA1_LENGTH] = '\0';

    sha1 = CStringGetDatum(cstring_to_text_with_len(formatted_commit_id, strlen(formatted_commit_id)));
    message = CStringGetDatum(cstring_to_text_with_len(commit_message, strlen(commit_message)));
    name = CStringGetDatum(cstring_to_text_with_len(commit_author->name, strlen(commit_author->name)));
    email = CStringGetDatum(cstring_to_text_with_len(commit_author->email, strlen(commit_author->email)));

    date = (commit_author->when.time * 1000000L) - POSTGRES_TO_UNIX_EPOCH_USECS;

    git_commit_free(commit);

    ExecClearTuple(slot);

    position = 0;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;
    slot->tts_isnull[position++] = false;

    position = 0;
    slot->tts_values[position++] = sha1;
    slot->tts_values[position++] = message;
    slot->tts_values[position++] = name;
    slot->tts_values[position++] = email;
    slot->tts_values[position++] = date;
    slot->tts_values[position++] = insertions;
    slot->tts_values[position++] = deletions;
    slot->tts_values[position++] = files_changed;

    ExecStoreVirtualTuple(slot);
    return slot;
  }
  else
  {
    festate->repo = NULL;
    festate->walker = NULL;
    return NULL;
  }
}

static void gitReScanForeignScan(ForeignScanState *node)
{
}

static void gitEndForeignScan(ForeignScanState *node)
{
  GitFdwExecutionState *festate = (GitFdwExecutionState *)node->fdw_state;
  git_repository_free(festate->repo);
  festate->repo = NULL;
  festate->walker = NULL;
  git_libgit2_shutdown();
}

static void estimate_costs(PlannerInfo *root,
                           RelOptInfo *baserel, GitFdwPlanState *fdw_private, Cost *startup_cost, Cost *total_cost)
{

  BlockNumber pages = fdw_private->pages;
  double ntuples = fdw_private->ntuples;
  Cost run_cost = 0;
  Cost cpu_per_tuple;

  *startup_cost = baserel->baserestrictcost.startup;

  run_cost += seq_page_cost * pages;
  cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
  run_cost += cpu_per_tuple * ntuples;

  *total_cost = *startup_cost + run_cost;
}

#if (PG_VERSION_NUM >= 90500)
static List *gitImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
  ListCell *lc;
  List *commands = NIL;
  char *path = "",
       *branch = "",
       *git_search_path = "",
       *prefix = "";
  StringInfoData cft_stmt;

  if (strcmp(stmt->remote_schema, "git_data") != 0)
  {
    ereport(ERROR,
            (errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
             errmsg("Foreign schema \"%s\" is invalid", stmt->remote_schema)));
  }

  foreach (lc, stmt->options)
  {
    DefElem *def = (DefElem *)lfirst(lc);

    if (strcmp(def->defname, "path") == 0)
      path = defGetString(def);
    else if (strcmp(def->defname, "branch") == 0)
      branch = defGetString(def);
    else if (strcmp(def->defname, "git_search_path") == 0)
      git_search_path = defGetString(def);
    else if (strcmp(def->defname, "prefix") == 0)
      prefix = defGetString(def);
    else
      ereport(ERROR,
              (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
               errmsg("invalid option \"%s\"", def->defname)));
  }

  if ((NULL != path) && (NULL != branch))
  {
    initStringInfo(&cft_stmt);

    appendStringInfo(&cft_stmt,
                     "CREATE FOREIGN TABLE %s.%srepository ("
                     "\n  sha1          text,"
                     "\n  message       text,"
                     "\n  name          text,"
                     "\n  email         text,"
                     "\n  commit_date   timestamp with time zone,"
                     "\n  insertions    int,"
                     "\n  deletions     int,"
                     "\n  files_changed int"
                     "\n)"
                     "\nSERVER %s"
                     "\nOPTIONS (path '%s',\n branch '%s',\n git_search_path '%s')",
                     stmt->local_schema,
                     prefix,
                     quote_identifier(stmt->server_name),
                     path,
                     branch,
                     git_search_path);

    commands = lappend(commands, pstrdup(cft_stmt.data));
    pfree(cft_stmt.data);
  }
  else
  {
    ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
             errmsg("Please set both `path` and `branch`")));
    return commands;
  }
  return commands;
}
#endif

bool gitAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
  *func = gitAcquireSampleRowsFunc;
  *totalpages = 1;
  return true;
}

typedef struct acquire_sample_rows_walker_state
{
  int target_rows;
  double *total_rows;
  double *dead_rows;
  int *numrows;
  HeapTuple *rows;
  TupleDesc tupDesc;
  Datum *values;
  bool *nulls;
} acquire_sample_rows_walker_state_t;

void acquire_sample_rows_callback(void *callback_state, callback_obj_t *obj)
{
  acquire_sample_rows_walker_state_t *cb_state = ((acquire_sample_rows_walker_state_t *)callback_state);

  switch (obj->type)
  {
  case CBT_ERROR:
    (*(cb_state->dead_rows))++;
  case CBT_COMMIT:
    for (int index = 0; index < cb_state->tupDesc->natts; ++index)
    {
      cb_state->nulls[index] = true;
    }

    if (*(cb_state->numrows) < cb_state->target_rows)
    {
      cb_state->rows[(*cb_state->numrows)++] = heap_form_tuple(cb_state->tupDesc, cb_state->values, cb_state->nulls);
    }
    (*cb_state->total_rows)++;
  }
}

int gitAcquireSampleRowsFunc(Relation relation,
                             int elevel,
                             HeapTuple *rows,
                             int targrows,
                             double *totalrows,
                             double *totaldeadrows)
{
  TupleDesc tupDesc;
  Datum *values;
  bool *nulls;
  GitFdwPlanState state;
  List *other_options;
  int numrows = 0;

  Assert(relation);
  Assert(targrows > 0);

  tupDesc = RelationGetDescr(relation);
  values = (Datum *)palloc(tupDesc->natts * sizeof(Datum));
  nulls = (bool *)palloc(tupDesc->natts * sizeof(bool));

  gitGetOptions(RelationGetRelid(relation), &state, &other_options);

  {
    acquire_sample_rows_walker_state_t iter_state = {
        targrows,
        totalrows,
        totaldeadrows,
        &numrows,
        rows,
        tupDesc,
        values,
        nulls};
    walkRepository(state.path,
                   state.branch,
                   state.git_search_path,
                   &iter_state,
                   acquire_sample_rows_callback);
  }

  pfree(values);
  pfree(nulls);

  ereport(elevel,
          (errmsg("\"%s\": repository contains %.0f rows; "
                  "%d rows in sample (was asked %d rows)",
                  RelationGetRelationName(relation),
                  *totalrows, numrows, targrows)));

  return numrows;
}

int walkRepository(const char *path,
                   const char *branch,
                   const char *git_search_path,
                   void *callback_state,
                   void (*callback)(void *, callback_obj_t *))
{
  git_repository *repo = NULL;
  git_remote *remote = NULL;
  git_oid oid = {0};
  int error;
  const git_remote_head **refs;
  size_t refs_len, i;
  git_remote_callbacks callbacks = GIT_REMOTE_CALLBACKS_INIT;
  git_revwalk *walker;
  git_libgit2_init();

  if (git_search_path != NULL)
  {
    git_libgit2_opts(
        GIT_OPT_SET_SEARCH_PATH,
        GIT_CONFIG_LEVEL_GLOBAL,
        git_search_path);
  }

  if (repo == NULL)
  {
    int repo_opened = -1;
    if ((repo_opened = git_repository_open(&repo, path)) != GIT_OK)
    {
      const git_error *err = giterr_last();
      ereport(ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg("Failed opening repository: '%s'", path),
               errdetail("libgit2 returned error code %d: %s.", repo_opened, err->message)));
      return 0;
    }

    error = git_remote_lookup(&remote, repo, path);
    if (error < 0)
    {
      error = git_remote_create_anonymous(&remote, repo, path);
      if (error < 0)
      {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Call to git_remote_create_anonymous failed"),
                        errdetail("Error code: %d", error)));
      }
    }

    error = git_remote_connect(
        remote,
        GIT_DIRECTION_FETCH,
        &callbacks,
        NULL
#if LIBGIT2_VER_MINOR > 24 || LIBGIT2_VER_MAJOR >= 1
        ,
        NULL
#endif
    );
    if (error < 0)
    {
      ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                      errmsg("Call to git_remote_connect failed"),
                      errdetail("Error code: %d", error)));
    }

    error = git_remote_ls(&refs, &refs_len, remote);
    if (error < 0)
    {
      ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                      errmsg("Call to git_remote_ls failed"),
                      errdetail("Error code: %d", error)));
    }

    for (i = 0; i < refs_len; i++)
    {
      if (0 == strcmp(refs[i]->name, branch))
      {
        oid = refs[i]->oid;
        break;
      }
    }

    if (git_oid_iszero(&oid))
    {
      ereport(ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg("Couldn't find branch %s", branch)));
    }

    git_remote_free(remote);
    git_revwalk_new(&walker, repo);
    git_revwalk_sorting(walker, GIT_SORT_TOPOLOGICAL | GIT_SORT_TIME);
    git_revwalk_push_head(walker);

    while (git_revwalk_next(&oid, walker) == 0)
    {
      git_commit *commit;
      int error = git_commit_lookup(&commit, repo, &oid);
      callback_obj_t obj;

      if (0 == error)
      {
        obj.type = CBT_COMMIT;
        obj.data = (void *)commit;
        (*callback)(callback_state, &obj);
        git_commit_free(commit);
      }
      else
      {
        obj.type = CBT_ERROR;
        obj.data = NULL;
        (*callback)(callback_state, &obj);
      }
    }
  }
  return 0;
}
