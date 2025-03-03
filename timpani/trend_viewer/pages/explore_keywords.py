import streamlit as st
import urllib.parse
import pandas as pd
import altair as alt

from timpani.trend_viewer.data_model import ViewerDataModel
from timpani.trend_viewer.sidebar_auth import TrendViewerSidebarAuth
from timpani.app_cfg import TimpaniAppCfg


class KeywordExplorer(object):
    """
    A filterable and explorable view of content keywords in the content store
    """

    def __init__(self, model: ViewerDataModel) -> None:
        self.model = model

    def run_ux(self):
        """
        Layout and initialize the streamlet components
        """
        st.set_page_config(
            page_title="Explore Keywords - Timpani Trend Viewer",
            layout="wide",
        )

        # create the sidebar content
        # return the authorizedselected workspaces
        sidebar = TrendViewerSidebarAuth(self.model, login_url=cfg.check_login_page_url)
        sidebar.authorized_workspace_selector()

        st.title("🏷️ Keywords")

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
            filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

        with filter_col1:
            # model names
            keyword_models = model.get_workspace_keyword_models()

            if len(keyword_models) > 0:
                selected_model = keyword_models[0]
                selected_model = st.selectbox(
                    "Keyword Model",
                    options=keyword_models,
                    index=keyword_models.index(selected_model),
                )
            else:
                selected_model = None

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

            # ----- LOAD DATA ------------
            # TODO: kind of strange to load in the middle of building ux, but df query
            # needs to be modified by params from above
            # Load up to 10,000 rows of data into the dataframe.
            # TODO need to filter by model

            keywords = model.get_workspace_keywords(
                model_name=selected_model,
                min_pub_date=published_date_range[0],
                max_pub_date=published_date_range[1],
                max_keywords=10000,
            )

            if len(keywords.index) > 0:
                # Notify the reader that the data was successfully loaded.
                data_load_state.text("")
                if len(keywords.index) == model.MAX_RETURN_ROWS:
                    data_load_state.text(
                        f"NOTE: a sample of {model.MAX_RETURN_ROWS} keywords from the full data have been loaded"
                    )
            else:
                data_load_state.text(
                    f"0 keywords loaded for workspace_id '{st.session_state.workspace_selected}'"
                )
                return
            # --- REMAINING FILTERS DEPEND AND FILTER LOADED DATA
            # keyword count range size range
            with filter_col3:
                item_count_range = st.slider(
                    "Keyword num items filter",
                    value=(2, max(keywords["num_items"])),
                    # need unfiltered slider range or cant unfilter data
                    min_value=1,
                    max_value=max(keywords["num_items"]),
                )

            # keyword num clusters filter
            with filter_col4:
                # num clusters  range
                num_clusters_range = st.slider(
                    "Keyword num clusters filter",
                    value=(
                        (max(0, min(keywords["num_clusters"]))),
                        (max(keywords["num_clusters"]) + 1),
                    ),
                    # TODO: need to get directoy from content store or can't reset filter
                    min_value=1,
                    max_value=max(keywords["num_clusters"]),
                )

        # ----- APPLY FILTERS ------
        # apply the subset filters
        keywords = keywords[keywords["num_items"] >= item_count_range[0]]
        keywords = keywords[keywords["num_items"] <= item_count_range[1]]
        keywords = keywords[keywords["num_clusters"] >= num_clusters_range[0]]
        keywords = keywords[keywords["num_clusters"] <= num_clusters_range[1]]

        # ----- GRID TABLE AND DETAIL VIEW ----
        # set up the columns
        col1, col2 = st.columns([3, 1])
        with col1:
            # show table of loaded data
            st.subheader(f"Content Keyword Groups ({len(keywords)})")

            hide_cols = [
                "keyword_model_name",
                "content_keyword_ids",
                "content_item_ids",
                "content_cluster_ids",
            ]
            display_data = keywords.drop(columns=hide_cols)
            show_cols = display_data.columns
            display_data.insert(0, "Select", False)
            # TODO: add columns with links through to content?
            edited_df = st.data_editor(
                display_data,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        required=True, width="small"
                    ),
                },
                # only enable editing for the selected column
                disabled=show_cols,
                use_container_width=True,
            )
            # TODO: this should become ids, but currently using text of keyword values
            selected_items = list(edited_df[edited_df.Select].keyword_text.values)

            if len(selected_items) > 0:
                # query to get the keywords items in the selected groups
                # TODO: add search by keyword string instead of post filtering?
                content_keywords = model.get_keywords(
                    model_name=selected_model,
                    keyword_texts=selected_items,
                )
                # TODO: this should be redundant now, filtering is in query
                content_keywords = content_keywords[
                    content_keywords["keyword_text"].isin(selected_items)
                ]

                hour_bins = (
                    content_keywords[
                        ["content_published_date", "content_item_id", "keyword_text"]
                    ]
                    # TODO: group counts by selected clusters
                    .groupby(
                        [
                            pd.Grouper(key="content_published_date", freq="D"),
                            "keyword_text",
                        ]
                    )
                    .count()
                    .reset_index()
                )

                # timeline chart
                with st.expander(
                    "Timeline of number of items in keyword group by day", expanded=True
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
                            y=alt.Y("content_item_id", title="num keywords"),
                            color=alt.Color("keyword_text", legend=None),
                        )
                    )
                    st.altair_chart(chart, use_container_width=True)
            with col2:
                st.subheader("Selected Keyword Details")
                if selected_items is not None and len(selected_items) > 0:
                    # set the cluster id selection
                    # TODO: allow multiple
                    grid_selected = keywords[
                        keywords["keyword_text"] == selected_items[0]
                    ]
                    search_keyword_id = ",".join(str(key) for key in selected_items)

                    #  write out table row data as a properties view
                    st.write(grid_selected.transpose())

                    # link to filters
                    keyword_content_url = f"/explore_content?content_keyword_id={urllib.parse.quote(search_keyword_id)}"
                    keyword_content_url += (
                        f"&workspace_id={st.session_state.workspace_selected}"
                    )

                    keyword_cluster_url = f"/explore_clusters?content_cluster_id={grid_selected['content_cluster_ids'].values[0]}"
                    keyword_cluster_url += (
                        f"&workspace_id={st.session_state.workspace_selected}"
                    )

                    st.markdown(
                        f""" <a href="{keyword_content_url}" target="_self">Go to content items with keyword</a><br/>
                            <a href="{keyword_cluster_url}" target="_self">Go to content clusters with keyword</a><br/>
                        """,
                        unsafe_allow_html=True,
                    )

                else:
                    st.write("No keyword selected")


if __name__ == "__main__":
    cfg = TimpaniAppCfg()
    model = ViewerDataModel()
    viewer = KeywordExplorer(model)
    viewer.run_ux()
