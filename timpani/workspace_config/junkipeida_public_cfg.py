from timpani.workspace_config.workspace_config import WorkspaceConfig


class JunkipediaPublicConfig(WorkspaceConfig):
    """
    Imports some example lists of concerning content from Junkipedia
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "QAnon_Pages": "/posts?lists=68",
    }

    def get_workspace_slug(self):
        return "junkipedia_public"

    def get_content_source_types(self):
        return ["junkipedia"]

    def get_queries(self, content_source_key: str):
        if content_source_key == "junkipedia":
            return self.junkipedia_queries
        else:
            return None

    def get_workflow_id(self):
        """
        NOTE: this is using the same workflow as the 'meedan' workspace, but storing in a different workspace
        TODO: should rename the workflow to the 'junkipedia' workflow?
        """
        return "meedan"

    def get_authed_check_workspace_slugs(self):
        return ["public"]
