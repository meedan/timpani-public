from timpani.workspace_config.workspace_config import WorkspaceConfig


class MeedanAAPIConfig(WorkspaceConfig):
    """
    Workspace profile for test processing AAPI twitter data store in s3 location
    https://s3.console.aws.amazon.com/s3/buckets/meedan-datasets?region=us-east-1&prefix=research-data/aapi_twitter/&showversions=false
    """

    # this is a 1.3GB file
    aws_s3_uri = "s3://meedan-datasets/research-data/aapi_twitter/tweets3.csv"

    # this is an alternate ~1Mb file for testing, 2500 lines, but 1114 content items due to embeded newlines
    # aws_s3_uri = "s3://meedan-datasets/research-data/aapi_twitter/tweets3_sample.csv"

    def get_workspace_slug(self):
        return "meedan_aapi"

    def get_content_source_types(self):
        return ["s3_csv_aapi_tweets"]

    def get_queries(self, content_source_key: str):
        """
        Since this is parsing s3 from csv, the 'query' is the path to the csv file
        """
        if content_source_key == "s3_csv_aapi_tweets":
            return {"tweets3.csv": self.aws_s3_uri}
        else:
            return None

    def get_workflow_id(self):
        return "meedan_aapi"

    def get_authed_check_workspace_slugs(self):
        return [
            "dev_public",
            "coinsights",
            "testing",
            "check-message-demo",
            "check-testing",
        ]
