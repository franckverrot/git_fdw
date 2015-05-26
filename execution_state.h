typedef struct GitFdwExecutionState
{
	char	   *path;
	char	   *branch;
	List	   *options;
	git_repository *repo;
	int passes;
	git_revwalk *walker;
} GitFdwExecutionState;
