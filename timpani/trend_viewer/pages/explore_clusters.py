import pandas as pd
import streamlit as st
import altair as alt
from timpani.app_cfg import TimpaniAppCfg
from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.trend_viewer.sidebar_auth import TrendViewerSidebarAuth
from timpani.trend_viewer.tweet_embed import TweetEmbed
from timpani.trend_viewer.telegram_embed import TelegramEmbed
from timpani.trend_viewer.youtube_embed import YouTubeEmbed
from timpani.trend_viewer.instagram_embed import InstagramEmbed
from timpani.trend_viewer.tiktok_embed import TikTokEmbed
from timpani.trend_viewer.facebook_embed import FacebookEmbed


class ClusterExplorer(object):
    """
    A filterable and explorable view of content clusters in the content store
    """

    def __init__(self, model: ViewerDataModel) -> None:
        self.model = model

    def run_ux(self):
        """
        Layout and initialize the streamlet components
        """
        st.set_page_config(
            page_title="Explore Clusters - Timpani Trend Viewer",
            layout="wide",
        )

        # create the sidebar content
        # return the authorizedselected workspaces
        sidebar = TrendViewerSidebarAuth(self.model, login_url=cfg.check_login_page_url)
        sidebar.authorized_workspace_selector()

        st.title("🗞️ Clusters")
        # hide the deploy button

        full_cluster_size_range = model.get_cluster_sizes_range()
        # initial values for filter, default to cluster size 2
        cluster_size_range = [
            max(2, full_cluster_size_range[0]),
            full_cluster_size_range[1],
        ]

        # set the query param from url if it was passed in
        if len(st.query_params.get_all("content_cluster_id")) > 0:
            search_cluster_id = st.query_params.get_all("content_cluster_id")[0]
        else:
            search_cluster_id = ""

        # Create a text element and let the reader know the data is loading.
        data_load_state = st.text("Loading data from content store...")

        # get the overall timerange for the workspace
        full_published_date_range = model.get_content_timerange()
        published_date_range = [
            full_published_date_range[0],
            full_published_date_range[1],
        ]

        # ----- QUERY FILTERS -------
        with st.expander("Filters", expanded=True):
            filter_col0, filter_col1, filter_col2, filter_col3, filter_col4 = (
                st.columns(5)
            )

            with filter_col0:
                # optionally sample from the table
                sample_filter = st.number_input(
                    label="Sample percent",
                    value=None,
                    step=0.01,  # need to be able to express small fractions for big datasets
                    min_value=0.0,
                    max_value=100.0,
                    help="Reduce size of large dataset by randomly sampling only a percentage of clusters",
                )

            # Cluster id search
            with filter_col1:
                search_cluster_id = st.text_input(
                    "Cluster id search", value=search_cluster_id
                )
                # convert possibly multiple values to list
                if search_cluster_id != "" and isinstance(search_cluster_id, str):
                    search_cluster_id = search_cluster_id.split(",")
                    search_cluster_id = list(map(int, search_cluster_id))

            with filter_col2:
                published_date_range = st.slider(
                    "Content published max date search",
                    value=(
                        published_date_range[0],
                        published_date_range[1],
                    ),
                    # need to get raw values from content store or can't reset filter
                    min_value=full_published_date_range[0],
                    max_value=full_published_date_range[1],
                )

            with filter_col3:
                # cluster size range
                cluster_size_range = st.slider(
                    "Cluster num items filter",
                    value=cluster_size_range,
                    # need unfiltered slider range or cant unfilter data
                    min_value=full_cluster_size_range[0],
                    max_value=full_cluster_size_range[1],
                )

            # ----- LOAD DATA ------------
            # TODO: kind of strange to load in the middle of building ux, but df query
            # needs to be modified by params from above

            # Load up to 10,000 rows of data into the dataframe.
            data = model.load_cluster_data(
                cluster_id_filter=search_cluster_id,
                min_cluster_size=cluster_size_range[0],
                max_cluster_size=cluster_size_range[1],
                min_pub_date=published_date_range[0],
                max_pub_date=published_date_range[1],
                sample_percent=sample_filter,
            )
            if len(data.index) > 0:
                # Notify the reader that the data was successfully loaded.
                data_load_state.text("")
                if len(data.index) == model.MAX_RETURN_ROWS:
                    data_load_state.text(
                        f"NOTE: a sample of {model.MAX_RETURN_ROWS} items from the full data have been loaded"
                    )
            else:
                data_load_state.text(
                    f"0 records loaded for workspace_id '{st.session_state.workspace_selected}'"
                )
                return

            # --- REMAINING FILTERS DEPEND AND FILTER LOADED DATA
            # unique cluster size filter
            with filter_col4:
                # cluster size range
                cluster_unique_size_range = st.slider(
                    "Cluster unique items filter",
                    value=(
                        (max(0, min(data["num_items_unique"]))),
                        (max(data["num_items_unique"]) + 1),
                    ),
                    # TODO: need to get directoy from content store or can't reset filter
                    min_value=1,
                    max_value=max(data["num_items_unique"]),
                )

        # ----- APPLY FILTERS ------
        # apply the subset filters

        data = data[data["min_pub_date"] >= pd.to_datetime(published_date_range[0])]
        data = data[data["max_pub_date"] <= pd.to_datetime(published_date_range[1])]

        data = data[data["num_items"] >= cluster_size_range[0]]
        data = data[data["num_items"] <= cluster_size_range[1]]

        data = data[data["num_items_unique"] >= cluster_unique_size_range[0]]
        data = data[data["num_items_unique"] <= cluster_unique_size_range[1]]

        # ----- GRID TABLE AND DETAIL VIEW ----
        # set up the columns
        col1, col2 = st.columns([3, 1])
        with col1:
            # show table of loaded data
            st.subheader(f"Content Clusters ({len(data)})")

            hide_cols = [
                "workspace_id",
                "exemplar_item_id",
                "stress_score",
                "priority_score",
                "updated_at",
                "created_at",
                "exemplar_published_url",
            ]
            display_data = data.drop(columns=hide_cols)
            show_cols = display_data.columns
            display_data.insert(0, "Select", False)
            edited_df = st.data_editor(
                display_data,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        required=True, width="small"
                    ),
                    "num_items": st.column_config.NumberColumn(
                        "num items", width="small"
                    ),
                    "num_items_added": st.column_config.NumberColumn(
                        "num items alltime", width="small"
                    ),
                    "num_items_unique": st.column_config.NumberColumn(
                        "num unique items", width="small"
                    ),
                    "content_cluster_id": st.column_config.TextColumn(
                        "cluster id", width="small"
                    ),
                    "exemplar_content": st.column_config.TextColumn(width="large"),
                },
                # only enable editing for the selected column
                disabled=show_cols,
                use_container_width=True,
            )
            selected_items = edited_df[edited_df.Select].content_cluster_id.values
            # TODO: now that timeline chart is down here, selected_items and search_cluster_id can be the same thing?
            if search_cluster_id is not None and search_cluster_id != "":
                data = data[data["content_cluster_id"].isin(search_cluster_id)]
                # query to get the content items for this cluster
                content_df = model.load_data(
                    cluster_id=search_cluster_id,
                )
                hour_bins = (
                    content_df[
                        [
                            "content_published_date",
                            "content_cluster_id",
                            "content_item_id",
                        ]
                    ]
                    # TODO: group counts by selected clusters
                    .groupby(
                        [
                            pd.Grouper(key="content_published_date", freq="D"),
                            "content_cluster_id",
                        ]
                    )
                    .count()
                    .reset_index()
                )

                # timeline chart
                with st.expander(
                    "Timeline of number of items in cluster by day", expanded=True
                ):
                    chart = (
                        alt.Chart(hour_bins)
                        .mark_line(point=True)
                        .encode(
                            x=alt.X(
                                "content_published_date",
                                # timeUnit="day",
                                title="date",
                            ),
                            y=alt.Y("content_item_id", title="num items"),
                            color=alt.Color("content_cluster_id", legend=None),
                        )
                    )
                    st.altair_chart(chart, use_container_width=True)

        with col2:
            st.subheader("Selected Cluster Details")

            if selected_items is not None and len(selected_items) > 0:
                tab1, tab2, tab3 = st.tabs(
                    ["Cluster Details", "Media Preview", "Keywords"]
                )
                with tab1:
                    # set the cluster id selection
                    grid_selected = data[
                        data["content_cluster_id"] == selected_items[0]
                    ]
                    search_cluster_id = ",".join(
                        str(clu) for clu in selected_items.tolist()
                    )

                    #  write out table row data as a properties view
                    st.write(grid_selected.transpose())

                with tab2:
                    # figure out what kind of url it is to embed appropriately
                    url = grid_selected["exemplar_published_url"].item()
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
                        ):  # TODO: also https://t.co
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

                with tab3:
                    # get the keywords for the selected item
                    # TODO: need to filter on keyword model
                    cluster_keywords = model.get_workspace_keywords(
                        content_cluster_ids=grid_selected[
                            "content_cluster_id"
                        ].to_list(),
                    )
                    if len(cluster_keywords) > 0:
                        # TODO: add a column with link through to keywords
                        st.write(
                            cluster_keywords[
                                [
                                    "keyword_text",
                                    "keyword_model_name",
                                    "num_items",
                                    "num_clusters",
                                ]
                            ]
                        )
                    else:
                        st.text("The items in this cluster have no keywords attached")

                # link to filters
                cluster_url = f"/explore_content?content_cluster_id={grid_selected['content_cluster_id'].item()}"
                cluster_url += f"&workspace_id={st.session_state.workspace_selected}"
                # link to compare
                compare_url = (
                    f"/explore_clusters?content_cluster_id={search_cluster_id}"
                )
                compare_url += f"&workspace_id={st.session_state.workspace_selected}"

                st.markdown(
                    f""" <a href="{cluster_url}" target="_self">Go to content items in cluster</a>
                    <a href="{compare_url}" target="_self">Compare trends</a>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.write("No cluster selected")

        # TODO: this should execute a query to fetch content items in this cluster
        # and show timeline plot?


if __name__ == "__main__":
    cfg = TimpaniAppCfg()
    model = ViewerDataModel()
    viewer = ClusterExplorer(model)
    viewer.run_ux()
