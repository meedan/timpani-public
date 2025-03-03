from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanNAWAGazaConfig(WorkspaceConfig):
    """
    Lists of example content related to conflict in Gaza
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "nawa_gaza_sources_2023": "/posts?lists=7007,7003,7014",
    }

    def get_workspace_slug(self):
        return "meedan_nawa_gaza"

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
        """
        return "meedan"

    def get_authed_check_workspace_slugs(self):
        return ["dev_public", "testing", "check-message-demo", "check-testing"]
