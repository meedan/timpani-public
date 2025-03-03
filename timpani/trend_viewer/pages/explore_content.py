import streamlit as st


# from streamlit_extras.switch_page_button import switch_page
import pandas as pd
from timpani.trend_viewer.tweet_embed import TweetEmbed
from timpani.trend_viewer.telegram_embed import TelegramEmbed
from timpani.trend_viewer.youtube_embed import YouTubeEmbed
from timpani.trend_viewer.instagram_embed import InstagramEmbed
from timpani.trend_viewer.tiktok_embed import TikTokEmbed
from timpani.trend_viewer.facebook_embed import FacebookEmbed
from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.trend_viewer.sidebar_auth import TrendViewerSidebarAuth

from timpani.app_cfg import TimpaniAppCfg


class ContentExplorer(object):
    """
    A filterable and searchable view of individual content items in the content store
    """

    def __init__(self, model: ViewerDataModel) -> None:
        self.model = model

    def run_ux(self):
        """
        Layout and initialize the streamlet components
        """
        st.set_page_config(
            page_title="Explore Content Items - Timpani Trend Viewer ",
            layout="wide",
        )

        # create the sidebar content
        # return the authorizedselected workspaces
        sidebar = TrendViewerSidebarAuth(self.model, login_url=cfg.check_login_page_url)
        sidebar.authorized_workspace_selector()

        st.title(
            "📝 Content",
            help="This view shows filterable tables of individual 'content items' (social media posts, etc)",
        )

        # Create a text element and let the reader know the data is loading.
        data_load_state = st.text("Loading data from content store...")

        # get the overall timerange for the workspace
        full_published_date_range = model.get_content_timerange()
        published_date_range = [
            full_published_date_range[0],
            full_published_date_range[1],
        ]

        # get the overall set of query ids for the workspace
        full_query_ids = model.get_content_query_ids()
        query_id_filter = full_query_ids

        # model names
        keyword_models = model.get_workspace_keyword_models()
        selected_model = None

        selected_keywords = []
        for key in st.query_params.get_all("content_keyword_id"):
            selected_keywords.extend(key.split(","))

        # default values for text search
        search_string = st.query_params.get("search_string")

        with st.expander(
            "Search & Filters",
            expanded=True,
        ):
            # time slider for selecting a sub range

            # ----- SEARCH FIELDS (WILL QUERY DB) -------
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                published_date_range = st.slider(
                    "Content published date search",
                    value=(
                        published_date_range[0],
                        published_date_range[1],
                    ),
                    # need to get raw values from content store or can't reset filter
                    min_value=full_published_date_range[0],
                    max_value=full_published_date_range[1],
                    help="Filter to show only content published within specific time period",
                )

            with search_col2:
                # optionally sample from the table
                sample_filter = st.number_input(
                    label="Sample percent",
                    value=None,
                    step=0.01,  # need to be able to express small fractions for big datasets
                    min_value=0.0,
                    max_value=100.0,
                    help="Reduce size of large dataset by randomly sampling only a percentage of items",
                )

            # set up cols for next row of filters
            filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

            with filter_col1:
                # cluster id
                # set the query param from url if it was passed in
                if len(st.query_params.get_all("content_cluster_id")) > 0:
                    search_cluster_id = st.query_params.get_all("content_cluster_id")[0]
                else:
                    search_cluster_id = ""
                search_cluster_id = st.text_input(
                    "Cluster id search",
                    value=search_cluster_id,
                    help="Show only content included in a specific list of cluster ids",
                )
                # TODO: how to invalitate the query parameter if cluster id changes?

                # Exact text search
                search_string = st.text_input(
                    "Content text search",
                    value=search_string,
                    help="Show only content that includes a specific text fragment (exact match)",
                )

            with filter_col2:

                with st.popover(
                    "Keywords Search",
                    help="Select keyword/topic model and keyword values to filter by",
                ):
                    if len(keyword_models) > 0:
                        selected_model = keyword_models[0]
                        selected_model = st.selectbox(
                            "Keyword Model",
                            options=keyword_models,
                            index=keyword_models.index(selected_model),
                        )
                    else:
                        selected_model = None
                    workspace_keywords = model.get_workspace_keywords(
                        model_name=selected_model,
                        keyword_texts=selected_keywords,  # filter list will only show search string if provided
                        max_keywords=100,  # otherwise only show top 100
                    )
                    # just want the value pairs for now
                    # TODO: this needs to show which model it is in as well?
                    full_keywords = workspace_keywords["keyword_text"].unique()
                    selected_keywords = st.multiselect(
                        label=f"Keyword Text Values ({selected_model})",
                        options=full_keywords,
                        default=selected_keywords,
                    )

            # query id (usually datasource or a seperate source type)
            with filter_col3:
                query_id_filter = st.multiselect(
                    label="Query id filter",
                    options=full_query_ids,
                    default=query_id_filter,
                    help="Show only data in a specific category defined by the data collection queries",
                )

            # ----- LOAD DATA -----
            # find out the total number of items in workspace
            workspace_size = model.get_workspace_size()
            # Load up to 10,000 rows of data into the dataframe.
            data = model.load_data(
                cluster_id=search_cluster_id,
                published_range_start=published_date_range[0],
                published_range_end=published_date_range[1],
                query_id=query_id_filter,
                keyword_ids=selected_keywords,
                text_match=search_string,
                sample_percent=sample_filter,
            )
            if len(data.index) > 0:
                # Notify the reader that the data was successfully loaded.
                if len(data.index) == model.MAX_RETURN_ROWS:
                    data_load_state.text(
                        f"""NOTE: The workspace {workspace_size} items but only {model.MAX_RETURN_ROWS} can be loaded at once.
                        Use the 'Sample percent' filter to randomly sample a smaller fraction of the workspace"""
                    )
                else:
                    data_load_state.text(
                        f"Search loaded {len(data.index)} items (of {workspace_size})"
                    )
            else:
                data_load_state.text(
                    f"Search loaded 0 records for workspace_id '{st.session_state.workspace_selected}'"
                )
                return
            # confirm dates are non-null
            # TODO: better to just filter out null values?
            if not all(pd.notnull(data["content_published_date"])):
                # lots of downstream stuff will break, so lets just exit here
                st.text("Content items have no non-null published dates")
                return

            # ----- FILTERS UX (REMOVE LOADED ITEMS FROM DATA FRAME)
            # TODO: make these all search

            # Value filter
            # TODO: filter values should come from original data or subset?
            lang_values = data["content_language_code"].unique()
            with filter_col4:
                lang_filter = st.multiselect(
                    label="Language filter",
                    options=lang_values,
                    default=lang_values,
                )

        # ----- APPLY FILTERS TO DF------

        # TODO: date filter is both a query and subset filter, not sure which we will use
        # should usually be no-op be cause already filtered by query
        data = data[
            data["content_published_date"] > pd.to_datetime(published_date_range[0])
        ]
        data = data[
            data["content_published_date"] <= pd.to_datetime(published_date_range[1])
        ]

        # apply the subset filters
        if search_cluster_id is not None and search_cluster_id != "":
            data = data[data["content_cluster_id"] == int(search_cluster_id)]

        if search_string is not None and search_string != "":
            data = data[data["content"].str.contains(search_string)]

        data = data[data["content_language_code"].isin(lang_filter)]

        data = data[data["query_id"].isin(query_id_filter)]

        # ----- GRID TABLE AND DETAIL VIEW ----
        # set up the columns
        col1, col2 = st.columns([3, 1])
        with col1:
            # show table of loaded data
            st.subheader(
                f"Content Items (filtered to {len(data)})",
                help="This table shows content items that match the filter criteria",
            )

            # copy only the subset of columns we want to display
            hide_cols = [
                "run_id",
                "workspace_id",
                "source_id",
                "query_id",
                "date_id",
                "raw_created_at",
                "raw_content_id",
                "raw_content",
                "updated_at",
                "content_language_code",
                "content_locale_code",
                "content_item_state_id",
                "current_state",
                "completed_timestamp",
            ]
            display_data = data.drop(columns=hide_cols)
            show_cols = display_data.columns
            display_data.insert(0, "Select", False)
            edited_df = st.data_editor(
                display_data,
                hide_index=True,
                # only enable editing for the selected column
                disabled=show_cols,
                column_order=[
                    "Select",
                    "content",
                    "content_item_id",
                    "content_cluster_id",
                    "content_published_date",
                    "content_published_url",
                ],
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        required=True, width="small"
                    ),
                    "content": st.column_config.TextColumn(width="large"),
                    "content_item_id": st.column_config.TextColumn("id", width="small"),
                    "content_cluster_id": st.column_config.TextColumn(
                        "cluster id", width="small"
                    ),
                    "content_published_date": st.column_config.DatetimeColumn(
                        "published date", width="small"
                    ),
                    "content_published_url": st.column_config.TextColumn(
                        "published url", width="medium"
                    ),
                },
                use_container_width=True,
            )

            # grab items from selected checkbox
            selected_items = edited_df[edited_df.Select].content_item_id.values

            # ---- TIMELINE VIEW -----
            with st.expander(
                "Timeline of content item published timestamp by hour", expanded=True
            ):
                # show timeline of content publish dates and counts
                hour_bins = (
                    data[["content_published_date", "content_item_id"]]
                    .groupby(pd.Grouper(key="content_published_date", freq="h"))
                    .count()
                )
                st.bar_chart(
                    hour_bins,
                    height=100,
                )

        with col2:
            st.subheader(
                "Selected Item Details",
                help="Select an item in the table to show media preview, full properties, and keywords",
            )

            if selected_items is not None and len(selected_items) > 0:
                tab1, tab2, tab3 = st.tabs(
                    ["Item Details", "Keywords", "Media Preview"]
                )
                with tab1:
                    grid_selected = data[data["content_item_id"] == selected_items[0]]

                    #  write out table row data as a properties view
                    st.write(grid_selected.transpose())
                with tab2:
                    # ---- Keyword set (optional)
                    if full_keywords is not None:
                        # get the keywords for the selected item
                        # TODO: need to filter on keyword model
                        item_keywords = model.get_keywords(
                            content_item_ids=grid_selected["content_item_id"].to_list(),
                        )
                        if len(item_keywords) > 0:
                            # TODO: add a column with link through to keywords
                            st.write(
                                item_keywords[["keyword_text", "keyword_model_name"]]
                            )
                        else:
                            st.text("This item has no keywords attached")
                with tab3:
                    # ---- Media preview
                    # figure out what kind of url it is to embed appropriately
                    url = grid_selected["content_published_url"].item()
                    if url is not None:  # can be missing
                        if url.startswith("https://t.me/"):
                            # TODO: the telegram component doesn't work, due to script loading?
                            TelegramEmbed(url).component()
                            link_html = f"""
                            <a href='{url}' target="_blank" rel="noopener noreferrer">view on Telegram</a>
                            """
                            st.write(link_html, unsafe_allow_html=True)
                        elif url.startswith(
                            "https://twitter.com/"
                        ):  # TODO: also https://t.co  and X?
                            TweetEmbed(url).component()
                        elif url.startswith("https://www.youtube.com/"):
                            YouTubeEmbed(url).component()
                        elif url.startswith("https://www.instagram.com/"):
                            InstagramEmbed(url).component()
                        elif url.startswith("https://www.tiktok.com"):
                            TikTokEmbed(url).component()
                        elif url.startswith("https://www.facebook.com"):
                            FacebookEmbed(url).component()
                        else:
                            st.text(f"Unknown content format: {url}")

                # show link to view the cluster
                if grid_selected["content_cluster_id"].item() is not None:
                    # how clusterid this turn into a decimal?
                    url = f"/explore_clusters?content_cluster_id={ int(grid_selected['content_cluster_id'].item()) }"
                    url += f"&workspace_id={st.session_state.workspace_selected}"
                    st.markdown(
                        f'<a href="{url}" target="_self">Go to cluster for this item</a>',
                        unsafe_allow_html=True,
                    )
                # TODO: this is a placeholder for check import, not yet implemented
                # https://meedan.atlassian.net/browse/CV2-4140
                st.link_button(
                    "Import to Check workspace",
                    url="https://qa.checkmedia.org/testing/settings/workspace",
                )

            else:
                st.write("No item selected")


if __name__ == "__main__":
    cfg = TimpaniAppCfg()
    model = ViewerDataModel()
    viewer = ContentExplorer(model)
    viewer.run_ux()
