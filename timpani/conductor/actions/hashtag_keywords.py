from collections import namedtuple
import re
import requests
import json

from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_item import ContentItem
import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()

ScoredTag = namedtuple("ScoredTag", "text score")


class BasicKeywordsExtrator(object):
    """
    Supports extracting keywords using simple text transformations that do
    not require resource intensive models
    """

    app_cfg = TimpaniAppCfg()

    CALLBACK_URL = app_cfg.timpani_conductor_api_endpoint + "/add_keywords"

    def get_name(self) -> str:
        return "hashtags"

    def add_keywords_to_item(
        self,
        item: ContentItem,
        target_state: str,
    ):
        """
        Extracts hashtags a request to the conductor to annotate the content_item with the keywords
        """
        hashtags = self.get_hashtags(item.content)
        # doing this via callback because wanting to validate that approach for yake
        # but it is kind of silly since already have a link to the content store and could update directly
        # TODO: just update the db directly
        if len(hashtags) < 1:
            logging.debug(
                f"no hashtags found for content_item_id {item.content_item_id}"
            )
            # important that we still do callback tho, or it will be stuck in the state and keep cycling
        self._do_keywords_callback(
            workspace_id=item.workspace_id,
            content_item_id=item.content_item_id,
            keywords=hashtags,
            target_state=target_state,
        )

    def get_hashtags(self, text: str):
        """
        Return a list of tuples with all of the text that looks like #hashtags
        """
        # hashtags = re.findall(r"#.*?(?=\s|$)", text)
        hashtags = []
        tags = re.findall(r"(?:^|\s)#(\w+)", text)
        for tag in tags:
            # all numeric items are probably a counter not a hashtag
            if tag.isdigit():
                continue
            # TODO: ignore stuff that looks like urls? or score lower
            # BUG: whitespace regex spliting between charaters incorrectly for hindi
            # https://bugs.python.org/issue12731

            # prepend removed hash (how does this work for R to L?)
            hashtags.append(ScoredTag(f"#{tag}", 1.0))

        return hashtags

    def _do_keywords_callback(
        self,
        workspace_id: str,
        content_item_id: str,
        keywords,
        target_state: str,
    ):
        """
        Helper function to make sure we do the keyword update callbacks in the same way
        """
        callback_response = requests.post(
            self.CALLBACK_URL + f"/{workspace_id}/{content_item_id}",
            data=json.dumps(
                {
                    "model_name": self.get_name(),
                    "state": target_state,
                    "keywords": keywords,
                }
            ),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Meedan Timpani/0.1 (Conductor)",  # TODO: cfg should know version
            },
        )
        assert (
            callback_response.ok
        ), f"Unable to process response from Timpani add keywords at {self.CALLBACK_URL} : {callback_response.text}"
