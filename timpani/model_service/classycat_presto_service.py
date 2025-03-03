from timpani.model_service.presto_wrapper_service import PrestoWrapperService
from timpani.content_store.content_item import ContentItem


import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class ClassycatPrestoService(PrestoWrapperService):
    """
    Wrapper for the classycat model service in presto.
    Most of the functionality for talking with Presto implemented in wrapper service
    """

    MODEL_KEY = "classycat"

    # TODO: This needs to be reimplemented with some of the stuff from the classywrapper, to support batch
    # https://meedan.atlassian.net/browse/CV2-4556
    def submit_to_presto_model(
        self, content_items: list[ContentItem], target_state=None
    ):
        logging.warning(
            f"Prestoclassycat not implemented so {len(content_items)} were not submited"
        )

    # def submit_to_presto_model  defined in superclass

    def parse_presto_response_result(self, result: dict):
        """
        TODO: copy functionality from classycat wrapper
        """
        labels = result["labels"]
        # give back keyword,score pairs to be added to item
        return labels
