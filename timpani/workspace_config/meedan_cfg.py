from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanConfig(WorkspaceConfig):
    """
    "real" profile for Meedan organization
    """

    junkipedia_queries = {
        # get the specific set of junkipedia lists
        "gendered_disinfo": "/posts?lists=5835,5836,7548",
        "ia_accounts": "/posts?lists=5861,5860,5865",
        "aapi_accounts": "/posts?lists=5426,5425",
        "cn_accounts": "/posts?lists=5863,5864",
        "vn_accounts": "/posts?lists=5862,7483",
    }

    def get_workspace_slug(self):
        return "meedan"

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
        return ["public"]
