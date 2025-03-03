from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanUS2024Config(WorkspaceConfig):
    """
    Workspace for testing out US 2024 Election related content feeds.
    Uses the shared Meedan workflow
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "tw_general_words": "/posts?lists=7550",
        "yt_general_words": "/posts?lists=7553",
        "tw_misinfo_words": "/posts?lists=7537",
        "yt_misinfo_words": "/posts?lists=7536",
    }

    def get_workspace_slug(self):
        return "meedan_us2024"

    def get_content_source_types(self):
        return ["junkipedia"]

    def get_queries(self, content_source_key: str):
        if content_source_key == "junkipedia":
            return self.junkipedia_queries
        else:
            return None

    def get_workflow_id(self):
        return "meedan"

    def get_authed_check_workspace_slugs(self):
        return ["dev_public", "testing", "check-message-demo", "check-testing"]
