"""
Test out the process of using Meedan's Check GraphQL API endpoint to determine
If a session is logged into Check and which workspaces it has access to.
"""

import argparse
import requests
import json
from timpani.workspace_config.workspace_cfg_manager import WorkspaceConfigManager
from timpani.app_cfg import TimpaniAppCfg

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class CheckAPISessionAuth(object):
    """
    Permissions model:
    * By default, authentication via Meedan Check is enabled
    * Authentication can be disabled by changing the value of TIMPANI_AUTH_MODE env variable
    * If auth is disabled, the set of workspace ids appearing in the database will be shown

    The meedan_check permissions model works as follows:
    * Each Timpani workspace declares a mapping to the  names (slugs) of any Check workspaces that should be granted access
    * Any Check users with acess to a mapped Check workspace can access the Timpani workspace
    * Users login to Check, which sets the appropriate session cookie auth token, visable to the domain
    * The trend viewer streamlit app gets the session cookie from the browser, and forwards it to the trend_viewer backend
    * Timpani calls the Check API to ask what workspaces the logged in user is allowed
    to access, forwarding the auth token for permission evaluation
    * Timpani matches the list of authorized Check workspaces against each workspace's
    access declarations
    * The value `public` means there are no access controls on the workspace
    * The value `dev-public` means that the workspaces can be accessed publically in dev enviornment

    If the meedan_auth is enabled, but _checkdesk_session cookie is in not present, or check-api is
    unreachable, only `public` and `dev-public` workspaces should be visible as appropriate,
    and workspace ids not appearing in mappings (i.e. some test data) will not appear.

    Note:  the name of the session cookie is now determined by an SSM environment variable for Check.
    If this value is changed, the integration with Timpani could break
    """

    cfg = TimpaniAppCfg()
    CHECK_GRAPHQL_BASE_URL = cfg.check_graphql_base_url
    CHECK_LOGIN_PAGE_URL = cfg.check_login_page_url

    def __init__(self) -> None:
        self.workspace_cfg = WorkspaceConfigManager()
        # loop over all the workspaces and get their auth mappings
        # so we can later look up timpani workspaces from check workspaces
        # but invert the mapping to check_workspace:[timpani workspaces]
        mappings = {}
        for workspace_id in self.workspace_cfg.get_all_workspace_ids():
            try:
                check_slugs = self.workspace_cfg.get_config_for_workspace(
                    workspace_id
                ).get_authed_check_workspace_slugs()
                for slug in check_slugs:
                    if mappings.get(slug) is None:
                        mappings[slug] = [workspace_id]
                    else:
                        mappings[slug].append(workspace_id)

            except NotImplementedError:
                logging.error(
                    f"Workspace {workspace_id} must implement get_authed_check_workspace_slugs() to indicate access permissions"
                )
        self.check_workspace_mappings = mappings
        # logging.debug(f"check workspace mappings: {self.check_workspace_mappings}")

    def get_session_authorized_check_workspaces(self, session_secret):
        """
        Query the check graphql api for authorized workspaces and parse result
        """
        authed_workspaces = []
        if session_secret is None:
            logging.warning(
                "No session secret provided so no check-api query issued for authorization"
            )
        else:
            # query for the set of authorized check workspaces
            auth_workspace_query = (
                "query { me { teams { edges { node { id dbid name slug  } } } } }"
            )

            try:
                result = self.query_check_graphql(auth_workspace_query, session_secret)
                # expecting a structure like
                # {
                # "data": {
                #     "me": {
                #     "teams": {
                #         "edges": [
                #         {
                #             "node": {
                #             "id": "VGVhbS81\n",
                #             "dbid": 5,
                #             "name": "Check Demo (On-Prem)",
                #             "slug": "check-message-demo"
                #             }
                #         },
                #         {
                #             "node": {
                #             "id": "VGVhbS82MQ==\n",
                #             "dbid": 61,
                #             "name": "testing",
                #             "slug": "testing"
                #             }
                #         },
                #         {
                #             "node": {
                #             "id": "VGVhbS82OA==\n",
                #             "dbid": 68,
                #             "name": "Google fact check tools",
                #             "slug": "google-fact-check-tools"
                #             }
                #         }
                #         ]
                #     }
                #     }
                # }
                # }
                # or
                # {'data': {'me': None}}
                if result["data"]["me"] is not None:
                    for node in result["data"]["me"]["teams"]["edges"]:
                        authed_workspaces.append(node["node"]["slug"])
                else:
                    logging.warning(
                        f"Unexpected auth graphql query result structure:{result}"
                    )
            except Exception as e:
                logging.warning(f"Unable to authenticate Check session:{e}")
                # logging.exception(e)

        return authed_workspaces

    def query_check_graphql(self, query, session_secret):
        """
        Issue a query against Check's internal GraphQL API and return results
        """
        session_cookie_name = "_checkdesk_session"
        if self.cfg.deploy_env_label == "qa":
            session_cookie_name = "_checkdesk_session_qa"

        data = {"query": query}
        response = requests.post(
            self.CHECK_GRAPHQL_BASE_URL + "/api/graphql",
            data=json.dumps(data),
            headers={
                "User-Agent": "Meedan Timpani/0.1 (Trend Viewer)",  # TODO: cfg should know version
                "Content-Type": "application/json; charset=utf-8",
            },
            cookies={session_cookie_name: session_secret},
        )
        assert response.ok is True, f"response was {response}"
        response_json = json.loads(response.text)
        return response_json

    def get_session_authorized_timpani_workspaces(self, session_secret):
        """
        Get authed timpani workspaces by lookkup up authed check workspaces
        and consulting the mapping defined in each workspace. 'public' means
        it is always accessible, 'dev_public' means it is accessible within
        dev environment (this probably poor security, but how will we debug otherwise?)
        """
        timpani_authed = set([])
        check_authed = self.get_session_authorized_check_workspaces(session_secret)
        for check_slug in check_authed:
            timpani_slugs = self.check_workspace_mappings.get(check_slug)
            if timpani_slugs is not None:
                timpani_authed = set.union(timpani_authed, set(timpani_slugs))
        # append on anything authed as public
        public_authed = self.check_workspace_mappings.get("public")
        timpani_authed = set.union(timpani_authed, public_authed)
        # if we are in local-dev workspaces, append on anything authed as dev_public
        dev_authed = []
        if self.cfg.deploy_env_label in ["local", "dev"]:
            dev_authed = self.check_workspace_mappings.get("dev_public")
        timpani_authed = set.union(timpani_authed, dev_authed)
        return list(timpani_authed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="TimpaniWorkspaceAuth",
        description="Queries check api to determine workspace auth for session cookie",
        epilog="https://github.com/meedan/timpani#readme",
    )
    parser.add_argument(
        "-s",
        "--session_secret",
        help="value of _checkdesk_session cookie extracted from browser",
        required=True,
    )
    args = parser.parse_args()
    print(
        f"Checking workspace authorization via {CheckAPISessionAuth.CHECK_GRAPHQL_BASE_URL}..."
    )
    auth = CheckAPISessionAuth()
    check_workspaces = auth.get_session_authorized_check_workspaces(
        session_secret=args.session_secret
    )
    timpani_workspaces = auth.get_session_authorized_timpani_workspaces(
        session_secret=args.session_secret
    )
    print(f"Authorized Check workspace slugs: {check_workspaces}")
    print(f"Authorized Timpani workspaces: {timpani_workspaces}")
