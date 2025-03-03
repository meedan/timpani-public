import streamlit as st
import pandas as pd
import sentry_sdk
from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.trend_viewer.sidebar_auth import TrendViewerSidebarAuth
from timpani.app_cfg import TimpaniAppCfg

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class TrendViewer(object):
    """
    A test of streamlit application integration with content store and capabilities we will need
    """

    def __init__(self, model: ViewerDataModel) -> None:
        self.model = model

    def run_ux(self):
        """
        Layout and initialize the streamlet components
        """
        st.set_page_config(
            page_title="Timpani Trend Viewer - Overview",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        # create the sidebar content
        # return the authorizedselected workspaces
        sidebar = TrendViewerSidebarAuth(self.model, login_url=cfg.check_login_page_url)
        sidebar.authorized_workspace_selector()

        st.header("Workspace Overview")

        # Create a text element and let the reader know the data is loading.
        data_load_state = st.text("Loading data from content store...")
        # Load up to 10,000 rows of data into the dataframe.
        data = model.get_workspace_day_item_counts()
        if len(data) > 0:
            # Notify the reader that the data was successfully loaded.
            data_load_state.text("")
            if len(data.index) == model.MAX_RETURN_ROWS:
                data_load_state.text(
                    f"NOTE: a sample of {model.MAX_RETURN_ROWS} items from the full data been loaded"
                )

            st.text("Number of items in workspace published by day")

            if all(pd.notnull(data["published_day"])):
                st.bar_chart(
                    data, x="published_day", y="num_items", use_container_width=False
                )
            else:
                st.text("No data")

            # three column grid layout to hilite explore options
            st.subheader("Interesting Items")
            st.subheader("Interesting Clusters")

        else:
            data_load_state.text(
                f"0 records loaded for workspace_id '{st.session_state.workspace_selected}'"
            )


if __name__ == "__main__":
    cfg = TimpaniAppCfg()
    sentry_sdk.init(
        dsn=cfg.sentry_sdk_dsn,
        environment=cfg.deploy_env_label,
        traces_sample_rate=1.0,
    )
    model = ViewerDataModel()
    viewer = TrendViewer(model)
    viewer.run_ux()
