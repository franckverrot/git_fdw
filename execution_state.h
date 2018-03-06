typedef struct GitFdwExecutionState
{
	char	   *path;
	char	   *branch;
	char	   *git_search_path;
	List	   *options;
	git_repository *repo;
	int passes;
	git_revwalk *walker;
} GitFdwExecutionState;
