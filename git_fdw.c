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
#include "optimizer/var.h"
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
#define DEFAULT_BRANCH "master"

static void gitGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void gitGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *gitGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
    ForeignPath *best_path, List *tlist, List *scan_clauses);
static void gitBeginForeignScan(ForeignScanState *node, int eflags);
static void gitExplainForeignScan(ForeignScanState *node, ExplainState *es);
static TupleTableSlot *gitIterateForeignScan(ForeignScanState *node);
static void fileReScanForeignScan(ForeignScanState *node);
static void gitEndForeignScan(ForeignScanState *node);

static bool is_valid_option(const char *option, Oid context);
static void gitGetOptions(Oid foreigntableid, GitFdwPlanState *state, List **other_options);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
    GitFdwPlanState *fdw_private,
    Cost *startup_cost, Cost *total_cost);

Datum git_fdw_handler(PG_FUNCTION_ARGS) {
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);

  fdwroutine->GetForeignRelSize  = gitGetForeignRelSize;
  fdwroutine->GetForeignPaths    = gitGetForeignPaths;
  fdwroutine->GetForeignPlan     = gitGetForeignPlan;

  fdwroutine->BeginForeignScan   = gitBeginForeignScan;
  fdwroutine->IterateForeignScan = gitIterateForeignScan;
  fdwroutine->EndForeignScan     = gitEndForeignScan;
  fdwroutine->ExplainForeignScan = gitExplainForeignScan;

  fdwroutine->ReScanForeignScan  = fileReScanForeignScan;

  PG_RETURN_POINTER(fdwroutine);
}

Datum git_fdw_validator(PG_FUNCTION_ARGS) {
  List     *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
  Oid      catalog = PG_GETARG_OID(1);
  char     *path   = NULL;
  char     *branch = NULL;
  List     *other_options = NIL;
  ListCell   *cell;

  foreach(cell, options_list) {
    DefElem    *def = (DefElem *) lfirst(cell);

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

      ereport(ERROR,
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
    else
      other_options = lappend(other_options, def);
  }

  if (catalog == ForeignTableRelationId && path == NULL) {
    elog(ERROR, "path is required for git_fdw foreign tables (path of the .git repo)");
  }

  PG_RETURN_VOID();
}

// FIXME: Avoid global variable
// TODO:  Find where this variable should go (Plan or Path?)
char * repository_path;
char * repository_branch;

static bool is_valid_option(const char *option, Oid context) {
  const struct GitFdwOption *opt;

  for (opt = valid_options; opt->optname; opt++) {
    if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
      return true;
  }
  return false;
}

// Fetch path and options from the server (only)
static void gitGetOptions(Oid foreigntableid, GitFdwPlanState *state, List **other_options) {
  ForeignTable *table;
  List     *options;
  ListCell   *lc,
             *prev;

  table = GetForeignTable(foreigntableid);

  options = NIL;
  options = list_concat(options, table->options);

  prev = NULL;
  foreach(lc, options) {
    DefElem *def = (DefElem *) lfirst(lc);

    if (strcmp(def->defname, "path") == 0) {
      state->path = defGetString(def);
      options = list_delete_cell(options, lc, prev);
    }

    if (strcmp(def->defname, "branch") == 0) {
      state->branch = defGetString(def);
      options = list_delete_cell(options, lc, prev);
    }

    prev = lc;
  }

  if (state->path == NULL) {
    elog(ERROR, "path is required for git_fdw foreign tables (path of the .git repo)");
  }

  if (state->branch == NULL) {
    state->branch = DEFAULT_BRANCH;
  }

  *other_options = options;
}

static void gitGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  GitFdwPlanState *fdw_private;

  // Pass path and options from the options (only the table for now, but
  // could be from the server too) into the baserel
  fdw_private = (GitFdwPlanState *) palloc(sizeof(GitFdwPlanState));
  gitGetOptions(foreigntableid, fdw_private, &fdw_private->options);
  baserel->fdw_private = (void *) fdw_private;
  // TODO: We should estimate baserel->rows
}

static void gitGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  GitFdwPlanState *fdw_private = (GitFdwPlanState *) baserel->fdw_private;
  Cost    startup_cost;
  Cost    total_cost;
  // List     *columns;
  List     *coptions = NIL;

  /* Estimate costs */
  estimate_costs(root, baserel, fdw_private, &startup_cost, &total_cost);

  /*
   * Create a ForeignPath node and add it as only possible path.  We use the
   * fdw_private list of the path to carry the convert_selectively option;
   * it will be propagated into the fdw_private list of the Plan node.
   */
  Path * path = (Path*)create_foreignscan_path(root, baserel,
      baserel->rows,
      startup_cost,
      total_cost,
      NIL,    /* no pathkeys */
      NULL,    /* no outer rel either */
      coptions);

  add_path(baserel, path);
}

static ForeignScan * gitGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses) {
  Index scan_relid = baserel->relid;
  scan_clauses = extract_actual_clauses(scan_clauses, false);

  best_path->fdw_private = baserel->fdw_private;
  ForeignScan * scan = make_foreignscan(tlist, scan_clauses, scan_relid, NIL, best_path->fdw_private);

  //ForeignScan * scan = make_foreignscan(tlist, scan_clauses, scan_relid, NIL, baserel->fdw_private);
  repository_path   = ((GitFdwPlanState*)scan->fdw_private)->path;
  repository_branch = ((GitFdwPlanState*)scan->fdw_private)->branch;
  return scan;
}

static void gitExplainForeignScan(ForeignScanState *node, ExplainState *es) {
}

static void gitBeginForeignScan(ForeignScanState *node, int eflags) {
  git_libgit2_init();
  // ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
  GitFdwExecutionState *festate;
  git_oid oid;

  festate = (GitFdwExecutionState *) palloc(sizeof(GitFdwExecutionState));
  festate->path   = repository_path;
  festate->branch = repository_branch;
  festate->repo   = NULL;
  festate->walker = NULL;

  node->fdw_state = (void *) festate;

  if(festate->repo == NULL) {
    int repo_opened = -1;
    if((repo_opened = git_repository_open(&festate->repo, festate->path)) != GIT_OK){
      elog(ERROR,"Failed opening repository: '%s' %d", festate->path, repo_opened);
      return;
    }

    // Read HEAD on specified branch (DEFAULT_BRANCH by default)
    char head_filepath[512];
    FILE *head_fileptr;
    char head_rev[41];

    strcpy(head_filepath, festate->path);
    if(strrchr(festate->path, '/') != (festate->path+strlen(festate->path)))
      strcat(head_filepath, "/refs/heads/");
    else
      strcat(head_filepath, "refs/heads/");
    strcat(head_filepath, festate->branch);

    if((head_fileptr = fopen(head_filepath, "r")) == NULL){
      elog(ERROR, "Error opening '%s'\n", head_filepath);
      return;
    }

    if(fread(head_rev, 40, 1, head_fileptr) != 1){
      elog(ERROR, "Error reading from '%s'\n", head_filepath);
      fclose(head_fileptr);
      return;
    }

    fclose(head_fileptr);

    if(git_oid_fromstr(&oid, head_rev) != GIT_OK){
      elog(ERROR,"Invalid git object: '%s'", head_rev);
      return;
    }

    git_revwalk_new(&(festate->walker), festate->repo);
    git_revwalk_sorting(festate->walker, GIT_SORT_TOPOLOGICAL);
    git_revwalk_push(festate->walker, &oid);
  } else {
    ereport(WARNING, (errcode(WARNING), errmsg("Repo is already initialized %p", festate->repo), errdetail("")));
  }
}

static TupleTableSlot * gitIterateForeignScan(ForeignScanState *node) {
  GitFdwExecutionState *festate = (GitFdwExecutionState *) node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

  git_oid oid;
  git_commit *commit;
  int position;

  ExecClearTuple(slot);

  const char          *commit_message;
  const git_signature *commit_author;
  const git_oid       *commit_sha1;

  if (git_revwalk_next(&oid, festate->walker) == GIT_OK) {
    if(git_commit_lookup(&commit, festate->repo, &oid)) {
      elog(ERROR, "Failed to lookup the next object\n");
      return NULL;
    }

    commit_message = git_commit_message(commit);
    commit_author  = git_commit_committer(commit);
    commit_sha1    = git_commit_id(commit);

    int padding = 1 /* prefix */ + 1 /* 0 */;
    char * commit_id    = (char*)palloc(40  + padding);
    char * commit_msg   = (char*)palloc(strlen(commit_message) + padding);
    char * author_name  = (char*)palloc(sizeof(char) * ((strlen(commit_author->name) + padding)));
    char * author_email = (char*)palloc(sizeof(char) * ((strlen(commit_author->email) + padding)));

    git_oid_fmt(commit_id + 1, commit_sha1);
    commit_id[0] = 's';
    Datum sha1 = CStringGetDatum(commit_id);

    sprintf(commit_msg, "s%s", commit_message);
    Datum message = CStringGetDatum(commit_msg);

    sprintf(author_name, "s%s", commit_author->name);
    Datum name = CStringGetDatum(author_name);

    sprintf(author_email, "s%s", commit_author->email);
    Datum email = CStringGetDatum(author_email);

    Datum date = (commit_author->when.time * 1000000L) - POSTGRES_TO_UNIX_EPOCH_USECS;

    git_commit_free(commit);

    slot->tts_isempty = false;
    slot->tts_nvalid = 5; // 5 columns = id, message, name, email, timestamp

    slot->tts_isnull = (bool *)palloc(sizeof(bool) * slot->tts_nvalid);
    slot->tts_values = (Datum *)palloc(sizeof(Datum) * slot->tts_nvalid);

    position = 0;
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

    ExecStoreVirtualTuple(slot);
    return slot;
  } else {
    festate->repo   = NULL;
    festate->walker = NULL;
    return NULL;
  }
}

static void fileReScanForeignScan(ForeignScanState *node) {
}

static void gitEndForeignScan(ForeignScanState *node) {
  GitFdwExecutionState *festate = (GitFdwExecutionState *) node->fdw_state;
  git_repository_free(festate->repo);
  festate->repo   = NULL;
  festate->walker = NULL;
  git_libgit2_shutdown();
}

static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel, GitFdwPlanState *fdw_private, Cost *startup_cost, Cost *total_cost)
{
  BlockNumber pages = fdw_private->pages;
  double    ntuples = fdw_private->ntuples;
  Cost    run_cost;
  Cost    cpu_per_tuple;

  *startup_cost = baserel->baserestrictcost.startup;

  // parsing is expensive, let's make it 100 times more expensive than seq scan
  run_cost = seq_page_cost * pages;
  cpu_per_tuple = cpu_tuple_cost * 100 + baserel->baserestrictcost.per_tuple;
  run_cost += cpu_per_tuple * ntuples;
  *total_cost = *startup_cost + run_cost;
}
