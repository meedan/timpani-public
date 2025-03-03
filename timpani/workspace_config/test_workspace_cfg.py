from timpani.workspace_config.workspace_config import WorkspaceConfig


class TestWorkspaceConfig(WorkspaceConfig):
    """
    Testing profile for CI tests etc
     # TODO: Add limits to content queries
     # TODO: implement a non http test content source with known content
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "query_1": "/posts?lists=5863"
    }

    def get_workspace_slug(self):
        return "test"

    def get_content_source_types(self):
        return ["junkipedia", "faker_testing"]

    def get_queries(self, content_source_key: str):
        if content_source_key == "junkipedia":
            return self.junkipedia_queries
        elif content_source_key == "faker_testing":
            return 910  # this just indicates how many fake items to construct
        else:
            return None

    def get_workflow_id(self):
        # TODO: this needs to return both test_workflow and default workflow, refactor to support multiple
        return "test_workflow"

    def get_authed_check_workspace_slugs(self):
        return ["testing", "check-testing"]
