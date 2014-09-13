typedef struct GitFdwPlanState
{
	char	   *path;
	List	   *options;
	BlockNumber pages;
	double	    ntuples;
} GitFdwPlanState;
