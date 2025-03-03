from typing import List
from timpani.content_sources.aws_s3_csv_content_source import AWSS3CSVContentSource
from timpani.raw_store.item import Item

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class AWSS3CSVAAPITweetsContentSource(AWSS3CSVContentSource):
    """
    Implements specific details of accessing the AAPI tweets csv file
    Scott: The original data is from the Twitter API 1 year ago.
    The derived fields in the CSV are cluster_id, dataset, and COVID-19 category.
    """

    def get_source_name(self):
        """
        This knows how to handle a specific csv format of AAPI-related
        tweeets stored in s3
        """
        return "s3_csv_aapi_tweets"

    def process_csv_chunk(
        self, chunk: List[str], workspace_id: str, query_id: str, page_id: str
    ):
        """
        Assumes input is the page result extracted from csv
        and ready to be split by lines.
        Parse each item of the 'data' array into a Raw Store
        and return as a payload array. Trying to delay parsing
        until later in the pipeline, but do need id fields extracted.

        Examples:
        author_id,conversation_id,created_at,edit_history_tweet_ids,id,lang,possibly_sensitive,referenced_tweets,
        text,entities.urls,public_metrics.impression_count,public_metrics.like_count,public_metrics.quote_count,
        public_metrics.reply_count,public_metrics.retweet_count,entities.mentions,attachments.media_keys,in_reply_to_user_id,
        entities.annotations,entities.hashtags,attachments.poll_ids,geo.place_id,clean_text,cluster_id,cluster_size,
        is_retweet,dataset,covid_id,covid_cat,covid_prob

        1360964483632496648,1598828291661578240,2022-12-02T23:55:44.000Z,{1598828291661578240},1598828291661578240,zh,f,"[{""id"":
        ""1598827602403160064"", ""type"": ""quoted""}]",8. 到2020年，連線的參與
        者刪除推文的請求是例行公事。 一位高管會寫信給另一位高管：“拜登團隊的更多
        評論。” 回覆將回來：“已處理”。
        https://t.co/VnInqMdywy,"[{""display_url"":
        ""twitter.com/mtaibbi/status\u2026"", ""end"": 93, ""expanded_url"":
        ""https://twitter.com/mtaibbi/status/1598827602403160064"", ""start"":
        70, ""url"": ""https://t.co/VnInqMdywy""}]",0,8,0,0,3,NaN,,,NaN,NaN,,,8.
        到2020年，連線的參與者刪除推文的請求是例行公事。 一位高管會寫信給另一位
        高管：“拜登團隊的更多評論。” 回覆將回來：“已處
        理”。,0,2,t,AAPI,8,Misrepresentation of Government
        Guidance,1.41577553749084

        1360964483632496648,1598827957325283328,2022-12-02T23:54:24.000Z,{1598827957325283328},1598827957325283328,zh,f,"[{""id"":
        ""1598825917165572099"", ""type"": ""quoted""}]",6. 然而，隨著時間的推
        移，該公司被迫慢慢增加這些壁壘。 一些最早控制言論的工具旨在打擊垃圾郵件
        和金融欺詐者等。 https://t.co/nd2vjPG02l,"[{""display_url"":
        ""twitter.com/mtaibbi/status\u2026"", ""end"": 80, ""expanded_url"":
        ""https://twitter.com/mtaibbi/status/1598825917165572099"", ""start"":
        57, ""url"": ""https://t.co/nd2vjPG02l""}]",0,4,0,0,4,NaN,,,NaN,NaN,,,6.
        然而，隨著時間的推移，該公司被迫慢慢增加這些壁壘。 一些最早控制言論的工
        具旨在打擊垃圾郵件和金融欺詐者等。,1,2,t,AAPI,1,The Origins of
        COVID-19,1.19265651702881

        1360964483632496648,1598827801313873920,2022-12-02T23:53:47.000Z,{1598827801313873920},1598827801313873920,zh,f,"[{""id"":
        ""1598826284477427713"", ""type"": ""quoted""}]",7. 慢慢地，隨著時間的推
        移，推特員工和高管開始為這些工具找到越來越多的用途。 局外人開始請願公司
        也操縱演講：首先是一點，然後是更頻繁，然後是不斷。
        https://t.co/aRCVKeYsoL,"[{""display_url"":
        ""twitter.com/mtaibbi/status\u2026"", ""end"": 98, ""expanded_url"":
        ""https://twitter.com/mtaibbi/status/1598826284477427713"", ""start"":
        75, ""url"":
        ""https://t.co/aRCVKeYsoL""}]",0,11,0,1,2,NaN,,,NaN,NaN,,,7. 慢慢地，隨
        著時間的推移，推特員工和高管開始為這些工具找到越來越多的用途。 局外人開
        始請願公司也操縱演講：首先是一點，然後是更頻繁，然後是不
        斷。,2,3,t,AAPI,3,"The Nature, Existence, and Virulence of
        SARS-CoV-2",1.4522420167923

        1360964483632496648,1598826858711171072,2022-12-02T23:50:02.000Z,{1598826858711171072},1598826858711171072,en,f,"[{""id"":
        ""1598825403182874625"", ""type"": ""retweeted""}]",RT @elonmusk: Here
        we go!! 🍿🍿,NaN,0,0,0,0,88358,"[{""end"": 12, ""id"": ""44196397"",
        ""start"": 3, ""username"": ""elonmusk""}]",,,NaN,NaN,,,: Here we go!!
        🍿🍿,3,29,t,AAPI,2,Transmission,1.33333361148834
        1360964483632496648,1598825308936880128,2022-12-02T23:43:53.000Z,{1598825308936880128},1598825308936880128,zh,f,NaN,"❤️🤍💙
        人民爱川爱得痴狂😂

        """
        header = [
            "author_id",
            "conversation_id",  # this is the id of the thread
            "created_at",
            "edit_history_tweet_ids",
            "id",  # This is the individual tweet id
            "lang",
            "possibly_sensitive",
            "referenced_tweets",
            "text",
            "entities.urls",
            "public_metrics.impression_count",
            "public_metrics.like_count",
            "public_metrics.quote_count",
            "public_metrics.reply_count",
            "public_metrics.retweet_count",
            "entities.mentions",
            "attachments.media_keys",
            "in_reply_to_user_id",
            "entities.annotations",
            "entities.hashtags",
            "attachments.poll_ids",
            "geo.place_id",
            "clean_text",
            "cluster_id",
            "cluster_size",
            "is_retweet",
            "dataset",
            "covid_id",
            "covid_cat",
            "covid_prob",
        ]
        payload = []
        badrows = 0

        for row in chunk:
            # for row in csv.DictReader(chunk, fieldnames=header):
            # couldn't get the DictReadier to work, so diy

            # if there are lots of bad rows, give up
            assert badrows < 100

            if len(row) == 0:
                logging.info("skipping blank empty row")
                badrows += 1
                continue

            if len(row) != len(header):
                logging.info(f"row is missing columns: {row}")
                badrows += 1
                continue

            rowdict = {}
            for i in range(len(row)):
                rowdict[header[i]] = row[i]

            # pull out the tweet id from the data
            # and then append everything else as a json blob

            # logging.info(f"row: {row}")
            # logging.info(f"rowdict: {rowdict}")

            content_id = rowdict["id"]  # individual tweet id
            item = Item(
                run_id=self.run_state.run_id,
                workspace_id=workspace_id,
                source_id=self.get_source_name(),
                query_id=query_id,
                page_id=page_id,
                content_id=content_id,
                content=rowdict,
            )
            payload.append(item)
            if badrows > 0:
                logging.warning(
                    f"Skipped {badrows} rows that did not map correctly from page {page_id}"
                )
        return payload
