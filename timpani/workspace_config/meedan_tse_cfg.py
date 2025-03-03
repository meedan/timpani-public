from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanTSEConfig(WorkspaceConfig):
    """
    Workspace profile for test processing TSE Tipline data store in s3 location
    https://s3.console.aws.amazon.com/s3/object/meedan-datasets?region=us-east-1&prefix=research-data/tse.csv
    """

    # this is a 47Mb file
    aws_s3_uri = "s3://meedan-datasets/research-data/tse.csv"

    def get_workspace_slug(self):
        return "meedan_tse"

    def get_content_source_types(self):
        return ["s3_csv_tse_tipline"]

    def get_queries(self, content_source_key: str):
        """
        Since this is parsing s3 from csv, the 'query' is the path to the csv file
        """
        if content_source_key == "s3_csv_tse_tipline":
            return {"tse.csv": self.aws_s3_uri}
        else:
            return None

    def get_workflow_id(self):
        return "meedan_tse"

    def get_authed_check_workspace_slugs(self):
        return ["dev_public", "testing", "check-message-demo", "check-testing"]
