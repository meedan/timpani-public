from typing import List
from timpani.content_sources.aws_s3_csv_content_source import AWSS3CSVContentSource
from timpani.raw_store.item import Item

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class AWSS3CSVTSETiplineContentSource(AWSS3CSVContentSource):
    """
    Implements specific details of accessing the TSE election tipline CSV file
    """

    def get_source_name(self):
        """
        This knows how to handle a specific  format of tipline data dumped to csv
        tweeets stored in s3
        """
        return "s3_csv_tse_tipline"

    def process_csv_chunk(
        self, chunk: List[str], workspace_id: str, query_id: str, page_id: str
    ):
        """
        Assumes input is the page result extracted from csv
        and ready to be split by lines.
        Parse each item of the 'data' array into a Raw Store
        and return as a payload array. Trying to delay parsing
        until later in the pipeline, but do need id fields extracted.

        NOTE: will probably strip out all of the non-text entries in the next steop

        Examples:
        id,feed_id,request_type,content,created_at,updated_at,request_id,media_id,medias_count,requests_count,last_submitted_at,webhook_url,
        last_called_webhook_at,subscriptions_count,fact_checked_by_count,project_medias_count,quote,type,file

        1097,3,text,Título ,2022-09-06T13:19:44.0211,2023-01-31T18:05:38.05533,,1388453,210,1266,2023-01-31T18:05:38.025553,,,32,5,2522,Título,Claim,

        452,3,text,Título de eleitor ,2022-09-05T23:24:59.858686,2023-01-31T17:22:12.279692,,1388188,176,747,2023-01-31T17:22:12.246763,,,10,5,1708,
        Título de eleitor,Claim,

        21364,3,video,null https://api.infobip.com/whatsapp/1/senders/556196371078/media/40b1ad8f-c041-4b4a-bb05-ade106ca3088,
        2022-09-23T10:58:57.210119,2022-09-23T10:58:57.210119,,1421093,1,1,2022-09-23T10:58:57.210119,,,0,0,0,,UploadedVideo,
        882967784dfc3f92aa32b725526cc655.mp4

        24167,3,audio,undefined https://api.infobip.com/whatsapp/1/senders/556196371078/media/e6b74faf-5ff4-4c6c-b9aa-11f8a9e0567c,
        2022-09-26T23:23:06.586336,2022-12-05T10:27:55.209435,,1426857,1,1,2022-09-26T23:23:06.586336,,,0,0,0,,
        UploadedAudio,6600c798ba473dd72bb879ce4552af46.mp3

        25670,3,video,null https://api.infobip.com/whatsapp/1/senders/556196371078/media/6f25398e-c100-45a0-84b1-8747b9935720,
        2022-09-27T22:15:32.442469,2022-12-05T10:34:35.900008,,150865,1,1,2022-09-27T22:15:32.442469,,,0,0,0,,
        UploadedVideo,d41d8cd98f00b204e9800998ecf8427e.mp4
        """
        header = [
            "id",
            "feed_id",
            "request_type",
            "content",
            "created_at",
            "updated_at",
            "request_id",
            "media_id",
            "medias_count",
            "requests_count",
            "last_submitted_at",
            "webhook_url",
            "last_called_webhook_at",
            "subscriptions_count",
            "fact_checked_by_count",
            "project_medias_count",
            "quote",
            "type",
            "file",
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

            content_id = rowdict["id"]  # individual id
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
