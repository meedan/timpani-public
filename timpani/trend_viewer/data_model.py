import pandas as pd
import datetime
from timpani.trend_viewer.pandas_content_store import PandasContentStore
from timpani.app_cfg import TimpaniAppCfg
from timpani.util.meedan_auth import CheckAPISessionAuth

# from timpani.content_store.content_store import ContentStore
from timpani.util.metrics_exporter import TelemetryMeterExporter

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ViewerDataModel(object):
    """
    Manages data loading and state for the Trend Viewer view
    TODO: coordinating data filters across views?
    """

    cfg = TimpaniAppCfg()

    MAX_RETURN_ROWS = 10000

    pd_content_store = None
    NO_WORKSPACE = "NO_WORKSPACE_AUTHORIZED"
    workspace_id = NO_WORKSPACE

    telemetry = TelemetryMeterExporter(service_name="timpani-trend_viewer")
    data_load_request_metric = telemetry.get_counter(
        # NOTE: this is *not* the number of items requested
        "data.requests",
        "number of individual requests made to load data from the content_store",
    )

    def __init__(
        self,
    ):
        # TODO: init with a ContentStore reference for dependency injection
        if self.pd_content_store is None:
            self.pd_content_store = PandasContentStore()
        self.auth = None

    def get_acessible_workspaces(self, auth_session_secret=None):
        """
        TODO: this function should check with some form of authorization
        (like Check API) to determine which workspaces the user can access
        """

        if self.cfg.timpani_auth_mode == "meedan_check":
            if self.auth is None:
                self.auth = CheckAPISessionAuth()
            # this will also include public, dev_public etc
            authed_workspaces = self.auth.get_session_authorized_timpani_workspaces(
                auth_session_secret
            )
            return authed_workspaces
        else:
            # return all the workspaces it finds in the database
            logging.warning(
                f"Auth mode is {self.cfg.timpani_auth_mode} (not enabled), all workspaces are accessible"
            )
            timpani_db_workspace = self.pd_content_store.get_workspaces()
            known_workspaces = timpani_db_workspace["workspace_id"]

            return known_workspaces.to_list()

    def set_workspace(self, workspace_id, auth_session_secret=None):
        """
        This is called whenever the workspace is changed to make sure
        logged in user has access
        """
        if self.cfg.timpani_auth_mode == "meedan_check":
            if self.auth is None:
                self.auth = CheckAPISessionAuth()
            authed_workspaces = self.auth.get_session_authorized_timpani_workspaces(
                auth_session_secret
            )
            assert (
                workspace_id in authed_workspaces
            ), f"Current session is not authorized to access workspace id: {workspace_id}"
            logging.debug(f"Check authorized access to workspace id {workspace_id}")
            self.workspace_id = workspace_id
            # indicate to caller that workspace was successfully set
            return workspace_id

        else:
            logging.warning(
                f"Auth mode is {self.cfg.timpani_auth_mode} (not enabled), all workspaces are accessible"
            )
            self.workspace_id = workspace_id
            # indicate to caller that workspace was successfully set
            return workspace_id
        return None

    def get_workspace_size(self):
        """
        Returns the number of content items (of any state within a workspace)
        """
        size_df = self.pd_content_store.get_workspace_size(self.workspace_id)
        if size_df is not None:
            return size_df["num_items"].iloc[0]

    def get_workspace_date_id_item_state_counts(self):
        """
        Returns a dataframe with number of content items by date_id
        """
        counts_df = self.pd_content_store.get_content_date_id_state_counts(
            self.workspace_id
        )
        return counts_df

    def get_workspace_day_item_counts(self):
        """
        Returns a dataframe with number of content items published each day
        """
        counts_df = self.pd_content_store.get_content_day_counts(self.workspace_id)
        return counts_df

    def get_workspace_process_states(self):
        """
        Returns a dataframe with process states
        """
        process_df = self.pd_content_store.get_process_states(self.workspace_id)
        return process_df

    def load_data(
        self,
        nrows=MAX_RETURN_ROWS,
        published_range_start=None,
        published_range_end=None,
        cluster_id=None,
        language_code=None,
        query_id=None,
        keyword_ids=None,
        text_match=None,
        sample_percent=None,
    ):
        """
        Fetch (and cache?) the dataframe describing the content items with one row per item

        """
        # TODO: need to iterate with chunk size if this gets big?
        if cluster_id == "":
            cluster_id = None
        elif isinstance(cluster_id, list):
            # make it into a comma delimited string
            cluster_id = ",".join(map(str, cluster_id))
        if query_id == "":
            query_id = None
        elif isinstance(query_id, list):
            # make it into a quoted comma delimited string
            query_id = ",".join(["'{}'".format(value) for value in query_id])
        # TODO: keyword ids currently are strings, will nee to changes to ids later
        if keyword_ids == "" or keyword_ids == []:
            keyword_ids = None
        elif isinstance(keyword_ids, list):
            # make it into a quoted comma delimited string
            keyword_ids = ",".join(["'{}'".format(value) for value in keyword_ids])
        if text_match == "":
            text_match = None
        if sample_percent == "":
            sample_percent = None

        content_items = self.pd_content_store.get_content_item_rows(
            workspace_id=self.workspace_id,
            max_limit=nrows,
            published_range_start=published_range_start,
            published_range_end=published_range_end,
            cluster_id=cluster_id,
            language_code=language_code,
            query_id=query_id,
            keyword_ids=keyword_ids,
            text_match=text_match,
            sample_percent=sample_percent,
        )
        self._log_access_metrics("load_data", self.workspace_id)
        # make sure there is data in the system to display
        if len(content_items) > 0:
            # format dates to dates
            # TODO: also format the other dates?
            content_items["content_published_date"] = pd.to_datetime(
                content_items["content_published_date"], format="%Y-%m-%dT%H:%M:%S.%f"
            )
        return content_items

    def load_cluster_data(
        self,
        cluster_id_filter=None,
        min_cluster_size=None,
        max_cluster_size=None,
        min_pub_date=None,
        max_pub_date=None,
        sample_percent=None,
        nrows=MAX_RETURN_ROWS,
    ):
        """
        Fetch a dataframe describing the content clusters with one row per cluster
        """
        # TODO: need to iterate with chunk size if this gets big?
        # debug
        if cluster_id_filter == "":
            cluster_id_filter = None
        elif isinstance(cluster_id_filter, list):
            # make it into a comma delimited string
            cluster_id_filter = ",".join(map(str, cluster_id_filter))
        if min_cluster_size is not None:
            min_cluster_size = int(min_cluster_size)
        if sample_percent == "":
            sample_percent = None

        content_clusters = self.pd_content_store.get_content_cluster_rows(
            workspace_id=self.workspace_id,
            cluster_id_str=cluster_id_filter,
            min_cluster_size=min_cluster_size,
            max_cluster_size=max_cluster_size,
            min_pub_date=min_pub_date,
            max_pub_date=max_pub_date,
            max_limit=nrows,
            sample_percent=sample_percent,
        )
        self._log_access_metrics("load_cluster_data", self.workspace_id)
        return content_clusters

    def get_workspace_keywords(
        self,
        model_name=None,
        content_cluster_ids=None,
        keyword_texts=None,
        min_pub_date=None,
        max_pub_date=None,
        max_keywords=None,
    ):
        """
        return the set of *unique* keywords in a workspace (possibly filtered)
        """
        if content_cluster_ids == "" or content_cluster_ids == []:
            content_cluster_ids = None
        elif isinstance(content_cluster_ids, list):
            # make it into a comma delimited string
            content_cluster_ids = ",".join(map(str, content_cluster_ids))
        if keyword_texts == "" or keyword_texts == []:
            keyword_texts = None
        elif isinstance(keyword_texts, list):
            # make it into a comma delimited string
            keyword_texts = ",".join(["'{}'".format(text) for text in keyword_texts])
        df = self.pd_content_store.get_aggregated_keywords(
            self.workspace_id,
            model_name=model_name,
            min_count=1,
            content_cluster_ids=content_cluster_ids,
            keyword_texts=keyword_texts,
            min_pub_date=min_pub_date,
            max_pub_date=max_pub_date,
            max_limit=max_keywords,
        )
        self._log_access_metrics("load_keyword_data", self.workspace_id)
        return df

    def get_workspace_keyword_models(self):
        """
        Return the list of keyword models for this workspace
        """
        df = self.pd_content_store.get_keyword_model_names(self.workspace_id)
        return df["keyword_model_name"].to_list()

    def get_keywords(
        self,
        model_name=None,
        content_item_ids=None,
        keyword_texts=None,
        start_date=None,
        end_date=None,
        max_limit=10000,
    ):
        """
        Return the data frame of keywords, filtered to worksapce and related filters
        """
        if content_item_ids == "":
            content_item_ids = None
        elif isinstance(content_item_ids, list):
            # make it into a comma delimited string
            content_item_ids = ",".join(map(str, content_item_ids))
        if keyword_texts == "":
            keyword_texts = None
        elif isinstance(keyword_texts, list):
            # make it into a comma delimited string
            keyword_texts = ",".join(["'{}'".format(text) for text in keyword_texts])

        df = self.pd_content_store.get_keywords(
            workspace_id=self.workspace_id,
            model_name=model_name,
            content_item_ids=content_item_ids,
            keyword_texts=keyword_texts,
            max_limit=max_limit,
        )
        if start_date is not None or end_date is not None:
            raise NotImplementedError
        return df

    def get_cluster_sizes_range(self):
        """
        Return a data frame holding the max and min values of clusters sizes (num_items)
        """
        df = self.pd_content_store.get_cluster_sizes_range(
            workspace_id=self.workspace_id
        )
        if df["min_size"][0] is not None:
            size_range = df["min_size"][0].item(), df["max_size"][0].item()
        else:
            size_range = 0, 0
        return size_range

    def get_content_timerange(self, time_field="content_published_date"):
        """
        Return a range with the earliest and latest published data associated with content
        """
        df = self.pd_content_store.get_content_timerange(
            workspace_id=self.workspace_id, time_field=time_field
        )
        if len(df.index) > 0 and df["min_date"][0] is not None:
            time_range = [
                df["min_date"][0].to_pydatetime(),
                df["max_date"][0].to_pydatetime(),
            ]
        else:
            time_range = [datetime.datetime.utcnow(), datetime.datetime.utcnow()]
        return time_range

    def get_content_query_ids(self):
        """
        Return the set of observed query ids on content items in specified workspace
        """
        query_ids = [None]
        df = self.pd_content_store.get_content_query_ids(workspace_id=self.workspace_id)
        if len(df.index) > 0:
            query_ids = df["query_id"].to_list()
        return query_ids

    def _log_access_metrics(self, caller_name: str, workspace_id: str):
        """
        record metrics indicating that the databaase access function caller_name has been called
        """
        if workspace_id is None:
            workspace_id = "unknown"
        self.data_load_request_metric.add(
            1,
            attributes={
                "caller_name": caller_name,
                "workspace_id": workspace_id,
            },
        )
