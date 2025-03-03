from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanIndiaElectionConfig(WorkspaceConfig):
    """
    Lists of example content from sources likley to share concerning content about the election in India
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "india_election_2024_twitter": "/posts?lists=7452",
        "india_election_2024_telegram": "/posts?lists=7453",
    }

    def get_workspace_slug(self):
        return "meedan_india_election"

    def get_content_source_types(self):
        return ["junkipedia"]

    def get_queries(self, content_source_key: str):
        if content_source_key == "junkipedia":
            return self.junkipedia_queries
        else:
            return None

    def get_workflow_id(self):
        """
        NOTE: We are testing out this workflow with classycat, won't work in production
        """
        return "meedan"

    def get_authed_check_workspace_slugs(self):
        return ["dev_public", "testing", "check-message-demo", "check-testing"]
