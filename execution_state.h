typedef struct GitFdwExecutionState
{
	char	   *path;
	List	   *options;
	git_repository *repo;
	int passes;
	git_revwalk *walker;
} GitFdwExecutionState;
