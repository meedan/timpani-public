import streamlit as st
import pandas as pd
import requests
from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.trend_viewer.sidebar_auth import TrendViewerSidebarAuth
from timpani.app_cfg import TimpaniAppCfg

# importing from conductor may break dependencies
from timpani.workspace_config.workspace_cfg_manager import WorkspaceConfigManager
from timpani.processing_sequences.workflow_manager import WorkflowManager


class StatusView(object):
    """
    Show various status about system and workspace state
    """

    cfg = TimpaniAppCfg()
    workspace_configs = WorkspaceConfigManager()
    workflows = WorkflowManager(content_store=None)

    def __init__(self, model: ViewerDataModel) -> None:
        self.model = model
        self.authed_workspaces = []

    def run_ux(self):
        """
        Layout and initialize the streamlet components
        """
        st.set_page_config(
            page_title="Workspace Status - Timpani Trend Viewer",
            layout="wide",
        )

        # create the sidebar content
        # return the authorizedselected workspaces
        sidebar = TrendViewerSidebarAuth(self.model, login_url=cfg.check_login_page_url)
        self.authed_workspaces = sidebar.authorized_workspace_selector()

        # ----- INITIAL DATA STATES --------

        st.header(f"Workspace Status: {st.session_state.workspace_selected}")
        st.text(
            "This page shows technical information about the workspace and the data processing workflow"
        )

        # Create a text element and let the reader know loaded data is processing.
        st.text("Status of the trend viewer is: OK")

        # query the conductor to check its state
        conductor_status = "unknown"
        try:
            conductor_status_url = (
                f"{self.cfg.timpani_conductor_api_endpoint}/healthcheck"
            )
            conductor_status_response = requests.get(conductor_status_url)
            conductor_status = conductor_status_response.text
        except requests.exceptions.ConnectionError as e:
            conductor_status = e
        st.text(f"Timpani Conductor status: {conductor_status}")

        date_id_counts = model.get_workspace_date_id_item_state_counts()
        if len(date_id_counts) > 0:
            st.write(
                "Number and status of items in workspace acquired for each date_id"
            )
            if all(pd.notnull(date_id_counts["num_items"])):
                st.bar_chart(
                    date_id_counts,
                    x="date_id",
                    y="num_items",
                    color="current_state",
                )
            else:
                st.text("No data")

        workspace = self.workspace_configs.get_config_for_workspace(
            st.session_state.workspace_selected
        )
        with st.expander("Workspace Details"):
            st.write(workspace)
        workflow = self.workflows.get_workflow(workspace.get_workflow_id())

        with st.expander("Workflow details"):

            gv_text = workflow.get_transition_graphviz(
                state_model=workflow.get_state_model()
            )
            st.graphviz_chart(gv_text)
            st.write(workflow)
            st.text("State model")
            st.text("valid states:")
            st.write(workflow.get_state_model().valid_states)
            st.text("valid transitions:")
            st.write(workflow.get_state_model().valid_transitions)

        st.text("Status of recent workflow processes")
        process_states = model.get_workspace_process_states()
        st.write(process_states)

        st.subheader("Active conductor processes:")
        try:
            conductor_running_process_url = (
                f"{self.cfg.timpani_conductor_api_endpoint}/running_processes"
            )
            running_process_response = requests.get(conductor_running_process_url)
            st.write(running_process_response.text)
        except requests.exceptions.ConnectionError as e:
            st.text(f"Error contacting conductor:{e}")


if __name__ == "__main__":
    cfg = TimpaniAppCfg()
    model = ViewerDataModel()
    viewer = StatusView(model)
    viewer.run_ux()
