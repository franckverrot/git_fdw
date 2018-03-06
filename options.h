struct GitFdwOption {
	const char *optname;
	Oid         optcontext;
};

// Only allow setting the repository's path
static const struct GitFdwOption valid_options[] = {
	{"path",   ForeignTableRelationId},
	{"branch", ForeignTableRelationId},
	{"git_search_path", ForeignTableRelationId},
	{NULL,     InvalidOid}
};
