import pandas as pd
from timpani.content_store.content_store import ContentStore
from timpani.app_cfg import TimpaniAppCfg
import streamlit as st


class PandasContentStore(object):
    """
    Executes *read only* sql queries against the content store database and returns
    results as pandas dataframes. Uses the engine from the content store.
    Uses streamlit's caching functionality to reduce how often queries are rerun.
    Any 'business logic' should happen a layer above in the data_model.
    Everything here will eventually be replaced by an API.

    Note that the caching stuff is tricky, idea is that query should not
    re execute if called with the same paramters within the TTL.
    https://docs.streamlit.io/library/advanced-features/caching
    TODO: input sanitization
    """

    CACHE_TIME_TO_LIVE = 3600  # cache is only valid for 1 hour

    content_store = None
    db_engine = None

    def __init__(self, content_store=None):
        """
        Get a read only database engine connection to the ContentStore
        """
        self.cfg = TimpaniAppCfg()
        if content_store is not None:
            self.content_store = content_store
            self.db_engine = self.content_store.ro_engine
        else:
            self.content_store = ContentStore()
            self.content_store.init_db_engine()
            self.db_engine = self.content_store.ro_engine

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_aggregated_keywords(
        _self,
        workspace_id,
        model_name=None,
        min_count=2,
        content_cluster_ids=None,
        keyword_texts=None,
        min_pub_date=None,
        max_pub_date=None,
        max_limit=10000,
    ):
        """
        Return the keywords and count values (grouped by string) for the workspace and model name
        with count values above min_count
        # TODO: maybe should not return all the content item ids, just a single exemplar?
        """
        model_name_clause = ""
        if model_name is not None:
            model_name_clause = f"and keyword_model_name = '{model_name}'"
        cluster_clause = ""
        if content_cluster_ids is not None:
            cluster_clause = f""" and ci.content_cluster_id in ({content_cluster_ids})
            """
        keyword_texts_clause = ""
        if keyword_texts is not None:
            keyword_texts_clause = f"and keyword_text in ({keyword_texts})"
        # NOTE: we are filtering only on max_pub_date, so that it it is possible to
        # set it to "show me keywords with recently added items" vs
        # "show me newly added keywords" (really needs 4 params!)
        min_pub_date_filter = ""
        if min_pub_date is not None:
            min_pub_date_filter = f"""    and min(ci.content_published_date) >= '{min_pub_date}'
                                """
        max_pub_date_filter = ""
        if max_pub_date is not None:
            max_pub_date_filter = f"""    and max(ci.content_published_date) <= '{max_pub_date}'
                                """
        limit_clause = ""
        if max_limit is not None:
            limit_clause = f"""   limit {max_limit}"""

        query = f"""select keyword_text,
                    keyword_model_name,
                    count(distinct content_keyword_id) num_items,
                    count(distinct content_cluster_id) num_clusters,
                    max(ci.content_published_date) max_published_date,
                    string_agg(cast(content_keyword_id as text), ',') content_keyword_ids,
                    string_agg(cast(ck.content_item_id as text), ',') content_item_ids,
                    string_agg(cast(ci.content_cluster_id as text), ',') content_cluster_ids
                    from content_keyword ck
                    join content_item ci on ck.content_item_id = ci.content_item_id
                    where ck.workspace_id = '{workspace_id}'
                    {model_name_clause}
                    {cluster_clause}
                    {keyword_texts_clause}
                    group by keyword_model_name, keyword_text
                    having count(distinct content_keyword_id) >= {min_count}
                    {min_pub_date_filter}
                    {max_pub_date_filter}
                    order by num_items desc, max_published_date
                    {limit_clause}
                """
        query_df = pd.read_sql_query(
            query,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_keywords(
        _self,
        workspace_id,
        model_name=None,
        content_item_ids=None,
        keyword_texts=None,
        start_date=None,
        end_date=None,
        max_limit=10000,
    ):
        """
        Return the individual keyword entries (per item) for the workspace and model name
        # TODO: maybe should not return all the content item ids, just a single exemplar?
        # TODO: range of content published dates
        """
        model_name_clause = ""
        if model_name is not None:
            model_name_clause = f"and keyword_model_name = '{model_name}'"
        content_items_clause = ""
        if content_item_ids is not None:
            content_items_clause = f"and content_item_id in ({content_item_ids})"
        keyword_texts_clause = ""
        if keyword_texts is not None:
            keyword_texts_clause = f"and keyword_text in ({keyword_texts})"

        query = f"""select *
                    from content_keyword
                    where workspace_id = '{workspace_id}'
                    {model_name_clause}
                    {content_items_clause}
                    {keyword_texts_clause}
                    limit {max_limit}
                """
        # TODO: start and end date filters
        query_df = pd.read_sql_query(
            query,
            con=_self.db_engine,
        )
        return query_df

    def get_keyword_model_names(_self, workspace_id):
        """
        Return a df with the set of keyword models included in a workspace
        """
        query = f"""select distinct keyword_model_name
                    from content_keyword
                    where workspace_id = '{workspace_id}'
                """
        query_df = pd.read_sql_query(
            query,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_item_rows(
        _self,
        workspace_id,
        # chunk_size=1000,
        max_limit=10000,
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
        Returns pandas dataframe describing the content items (including some joind data)
        Data is pseudo-random sample of max_limit items. A random seed is used to ensure
        that we get the same set of data on subsequent queries (until the underling data changes)
        TODO: probably need to implement chunksize and paging if this gets big, or will pandas handle?
        """

        time_filter = ""
        if published_range_start is not None or published_range_end is not None:
            time_filter = f"""
                        and ci.content_published_date >= '{published_range_start}'
                        and ci.content_published_date <= '{published_range_end}'
                        """
        cluster_filter = ""
        if cluster_id is not None:
            cluster_filter = f"and ci.content_cluster_id in ({cluster_id})"
        language_filter = ""
        if language_code is not None:
            language_filter = f"and ci.content_language_code = {language_code}"
        query_id_filter = ""
        if query_id is not None:
            query_id_filter = f"and ci.query_id in ({query_id})"
        keyword_clause = ""
        if keyword_ids is not None:
            # need to be careful not to introduce dupes due to multiple keywords per item
            # TODO: this is currently on keyword_text, eventually need to change to id
            keyword_clause = f"""
                               join (
                                        select distinct content_item_id from content_keyword
                                        where workspace_id = '{workspace_id}'
                                        and keyword_text in ({keyword_ids})
                                    ) as keylinks
                                    on ci.content_item_id = keylinks.content_item_id
                               """
        text_match_filter = ""
        if text_match is not None:
            # NOTE: percent signs used as wild cards '%' must be escaped '%%'
            text_match_filter = f"and ci.content like '%%{text_match}%%'"
        sample_clause = ""
        if sample_percent is not None:
            # Note: 'repeatable ()' is passing RNG seed to get consistant results as query is repeated
            sample_clause = f"tablesample system ({sample_percent}) repeatable (42)"

        query = f"""
                select ci.*, cis.current_state, cis.completed_timestamp
                    from content_item ci
                    {sample_clause}
                    join content_item_state cis
                        on ci.content_item_state_id = cis.state_id
                    {keyword_clause}
                    where workspace_id = '{workspace_id}'
                    {time_filter}
                    {cluster_filter}
                    {language_filter}
                    {query_id_filter}
                    {text_match_filter}
                    order by ci.content_item_id
                    limit {max_limit}
                """
        # print(query)
        query_df = pd.read_sql_query(
            query,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_timerange(_self, workspace_id, time_field="content_published_date"):
        """
        Return a dataframe  [min_date, max_date] with the time range for the content items in specified workspace
        TODO: support multiple time fields?
        """

        query_df = pd.read_sql_query(
            f"""
            select min(ci.{time_field}) min_date, max(ci.{time_field}) max_date
            from content_item ci
            where workspace_id ='{workspace_id}'
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_date_id_state_counts(_self, workspace_id):
        """
        Return a dataframe with counts number of content items acquired by date_id
        (when content was ingested) for the given workspace_id
        """

        query_df = pd.read_sql_query(
            f"""
            select date_id, cis.current_state, count(*) as num_items from content_item ci
            join content_item_state cis on ci.content_item_state_id = cis.state_id
            where workspace_id ='{workspace_id}'
            group by date_id, cis.current_state order by date_id
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_day_counts(_self, workspace_id):
        """
        Return a dataframe with counts number of content items published by
        day
        """

        query_df = pd.read_sql_query(
            f"""
            select date_trunc('day',ci.content_published_date) published_day, count(*) as num_items from content_item ci
            where workspace_id ='{workspace_id}'
            group by published_day order by published_day
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_process_states(_self, workspace_id):
        """
        Return a dataframe with status of running and completed processes for workspace
        """
        query_df = pd.read_sql_query(
            f"""
            select date_id, job_type, current_state, attempt_num,attempt_start,
            attempt_end, source_name ,query_id, run_id
            from process_state ps where workspace_id = '{workspace_id}'
            order by date_id desc, attempt_start desc
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_language_codes(_self, workspace_id):
        """
        Return a dataframe with the set of language codes on content items in specified workspace
        """
        query_df = pd.read_sql_query(
            f"""
            select distinct content_language_code from content_item
            where workspace_id ='{workspace_id}'
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_query_ids(_self, workspace_id):
        """
        Return a dataframe  [query_id] with the set of observed query ids on content items in specified workspace
        TODO: include counts
        """
        query_df = pd.read_sql_query(
            f"""
            select distinct(query_id) from content_item
            where workspace_id ='{workspace_id}'
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_content_cluster_rows(
        _self,
        workspace_id,
        # chunk_size=1000,
        max_limit=10000,
        cluster_id_str=None,  # comma delimted string
        min_cluster_size=None,
        max_cluster_size=None,
        min_pub_date=None,
        max_pub_date=None,
        sample_percent=None,
    ):
        """
        Returns pandas dataframe describing the content items (including some joined data)
        Data is pseudo-random sample of max_limit items. A random seed is used to ensure
        that we get the same set of data on subsequent queries (until the underling data changes)
        TODO: probably need to implement chunksize and paging if this gets big, or will pandas handle?

        """
        cluster_filter = ""
        if cluster_id_str is not None:
            cluster_filter = f"""    and ci.content_cluster_id in ({cluster_id_str})
                            """
        min_size_filter = ""
        if min_cluster_size is not None:
            min_size_filter = f"""    and num_items >= {min_cluster_size}
                                """
        max_size_filter = ""
        if max_cluster_size is not None:
            max_size_filter = f"""    and num_items <= {max_cluster_size}
                                """

        # NOTE: we are filtering only on max_pub_date, so that it it is possible to
        # set it to "show me clusters with recently added items" vs
        # "show me newly added clusters" (really needs 4 params!)
        min_pub_date_filter = ""
        if min_pub_date is not None:
            min_pub_date_filter = f"""    and content_agg.max_pub_date >= '{min_pub_date}'
                                """
        max_pub_date_filter = ""
        if max_pub_date is not None:
            max_pub_date_filter = f"""    and content_agg.max_pub_date <= '{max_pub_date}'
                                """
        sample_clause = ""
        if sample_percent is not None:
            # Note: 'repeatable ()' is passing RNG seed to get consistant results as query is repeated
            sample_clause = f"tablesample system ({sample_percent}) repeatable (42)"
        query = f"""
                with content_agg as (
                    select cc.content_cluster_id,
                    min(ci.content_published_date) min_pub_date,
                    max(ci.content_published_date) max_pub_date
                    from content_cluster cc join content_item ci
                        on ci.content_cluster_id = cc.content_cluster_id
                    where ci.workspace_id = '{workspace_id}'
                    group by cc.content_cluster_id
                )
                select cc.*,
                    ci."content" as exemplar_content,
                    ci.content_published_url as exemplar_published_url,
                    content_agg.max_pub_date,
                    content_agg.min_pub_date
                from content_cluster cc
                {sample_clause}
                join content_item ci on cc.exemplar_item_id = ci.content_item_id
                join content_agg on content_agg.content_cluster_id = cc.content_cluster_id
                where cc.workspace_id = '{workspace_id}'
                {cluster_filter}
                {min_size_filter}
                {max_size_filter}
                {min_pub_date_filter}
                {max_pub_date_filter}
                order by cc.content_cluster_id
                limit {max_limit}
                """
        query_df = pd.read_sql_query(
            query,
            con=_self.db_engine,
        )
        # print(query)
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_cluster_sizes_range(_self, workspace_id):
        """
        Return an array with the min,max size of clusters in the workspace
        """
        query_df = pd.read_sql_query(
            f"""
            select min(cc.num_items) min_size, max(cc.num_items) max_size
            from content_cluster cc
            where workspace_id='{workspace_id}'
            """,
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_workspaces(_self):
        """
        TODO: this should be removed and replaced with a call to Check API to list
        workspaces that logged in user has access to
        """
        query_df = pd.read_sql_query(
            "select distinct workspace_id from content_item",
            con=_self.db_engine,
        )
        return query_df

    @st.cache_data(ttl=CACHE_TIME_TO_LIVE)
    def get_workspace_size(_self, workspace_id):
        """
        TODO: confirm that user has acess to workspace
        """
        query_df = pd.read_sql_query(
            f"select count(*) as num_items from content_item where workspace_id='{workspace_id}'",
            con=_self.db_engine,
        )
        return query_df

    # not cached because for looking at current state
    def get_workspace_date_id_states(self, workspace_id):
        """
        Returns the number of items in each state by date_id
        (ingestion date key) for the workspace
        """
        query_df = pd.read_sql_query(
            f"""
            select date_id, cis.current_state , count(*) num_items
            from content_item ci
            join content_item_state cis
                on ci.content_item_state_id =cis.state_id
            where workspace_id='{workspace_id}'
            group by date_id, cis.current_state
            order by date_id
            limit 100
            """,
            con=self.db_engine,
        )
        return query_df
