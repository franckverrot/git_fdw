typedef struct GitFdwPlanState
{
	char	   *path;
	char	   *branch;
	char	   *git_search_path;
	List	   *options;
	BlockNumber pages;
	double	    ntuples;
} GitFdwPlanState;
