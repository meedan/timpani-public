import streamlit as st

from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.app_cfg import TimpaniAppCfg


class TrendViewerSidebarAuth(object):
    """
    Class to handle workspace selection and
    auth and sidebar display to be shared across the pages of the app
    """

    def __init__(self, model: ViewerDataModel, login_url) -> None:
        self.cfg = TimpaniAppCfg()
        self.model = model
        self.authed_workspaces = []
        self.login_url = login_url
        session_secret = self._get_session_secret()
        if session_secret is not None:
            self.authed_workspaces = self.model.get_acessible_workspaces(
                auth_session_secret=session_secret
            )

    def _get_session_secret(self):
        """
        Get the value of the _checkdesk_session cookie from the connection headers,
        if it exists.
        """
        # streamlit added functionality to get values from headers
        # https://docs.streamlit.io/develop/api-reference/utilities/st.context#contextheaders
        headers = st.context.headers
        cookies = headers.get("Cookie")
        session_secret = None
        if cookies is not None:
            for cookie in str.split(cookies, ";"):
                cookie = cookie.strip()
                # the name of the cookie can change per env and is stored in check-api/session_store_key in SSM parameter store
                if (
                    cookie.startswith("_checkdesk_session_qa=")
                    and self.cfg.deploy_env_label == "qa"
                ):
                    session_secret = cookie.replace("_checkdesk_session_qa=", "")
                elif (
                    cookie.startswith("_checkdesk_session=")
                    and self.cfg.deploy_env_label != "qa"
                ):
                    session_secret = cookie.replace("_checkdesk_session=", "")
        return session_secret

    def authorized_workspace_selector(self):
        # hide the deploy button
        st.markdown(
            r"""
            <style>
            .stDeployButton {
                    visibility: hidden;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

        session_secret = self._get_session_secret()

        # create the workspace selecter in the sidebar
        workspaces = self.model.get_acessible_workspaces(
            auth_session_secret=session_secret
        )
        if len(st.query_params.get_all("workspace_id")) > 0:
            st.session_state.workspace_selected = self.model.set_workspace(
                st.query_params.get_all("workspace_id")[0], session_secret
            )
        # select a default
        elif "workspace_selected" not in st.session_state:
            st.session_state.workspace_selected = self.model.set_workspace(
                workspaces[0], session_secret
            )

        # create the workspace selecter in the sidebar
        st.session_state.workspace_selected = st.sidebar.selectbox(
            "Selected Workspace",
            options=workspaces,
            index=workspaces.index(st.session_state.workspace_selected),
        )
        self.model.set_workspace(st.session_state.workspace_selected, session_secret)

        if self.cfg.timpani_auth_mode == "meedan_check":
            # TOOD: can we check if check login was sucessesful?  i.e. cookie may be be present but not logged in
            if session_secret is None:
                st.sidebar.markdown(
                    # TODO: we could replace this with javascript to automatically navigate back to trend_viewer
                    # from Check login as Caio suggests, but at least this forces user to go 'back' to the page
                    # once the login cookie is collected
                    f"Please <a href='{self.login_url}' target='_self'> login to Check</a> to authorize workspace access",
                    unsafe_allow_html=True,
                )
        return st.session_state.workspace_selected
