from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanLiteConfig(WorkspaceConfig):
    """
    Testing profile for meedan that includes a reduced set of
    queries, intented for QA testing
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "gendered_disinfo": "/posts?lists=5835,5836"
    }

    def get_workspace_slug(self):
        return "meedan-lite"

    def get_content_source_types(self):
        return ["junkipedia"]

    def get_queries(self, content_source_key: str):
        if content_source_key == "junkipedia":
            return self.junkipedia_queries
        else:
            return None

    # use the same workflow as meedan, just a reduced set of content
    def get_workflow_id(self):
        return "meedan"

    def get_authed_check_workspace_slugs(self):
        return ["public"]
