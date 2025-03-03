class WorkspaceConfig(object):
    """
    Class definition (mostly abstract) with the various functions and
    parameters needed to represent and store information about a specific workspace
    """

    def get_workspace_slug(self):
        """
        unique short string representation that can be used
        as an id to key things
        """
        raise NotImplementedError

    def get_content_source_types(self):
        """
        give back an array of content source names
        """
        raise NotImplementedError

    def get_queries(self, content_source_key: str):
        """
        return an dict of any queries that this workspace has stored that can
        be executed for the content source named in the key
        """
        raise NotImplementedError

    def get_workflow_id(self) -> str:
        """
        Return the name of processing workflow appropriate for this workspace
        TODO: this should support multiple workflows for different datasources
        """
        raise NotImplementedError

    def get_num_query_bins(self) -> int:
        """
        Return the number of splits a (usually daily) query should be devided
        into in order to reduce the size of individual query requests.
        Default is 1, i.e. no sub bins.
        """
        return 1

    def get_authed_check_workspace_slugs(self):
        """
        Return an array of slug ids for Check workspaces that
        are authorized to access the Timpani workspace (if any).
        Note: subclass must explicitly return "public" if login
        not required
        """
        raise NotImplementedError
