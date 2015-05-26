typedef struct GitFdwPlanState
{
	char	   *path;
	char	   *branch;
	List	   *options;
	BlockNumber pages;
	double	    ntuples;
} GitFdwPlanState;
