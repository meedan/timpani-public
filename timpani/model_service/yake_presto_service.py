from timpani.model_service.presto_wrapper_service import PrestoWrapperService

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class YakePrestoService(PrestoWrapperService):
    """
    Wrapper for the Yet Another Keyword Extractor model service in presto.
    Most of the functionality for talking with Presto implemented in wrapper service
    """

    MODEL_KEY = "yake_keywords"

    # TODO: probably this needs to be able to set apropriate parameter
    # values to forward to the model

    # def submit_to_presto_model  defined in superclass

    def parse_presto_response_result(self, result: dict):
        """
        Parse the YAKE model-specific details from the presto response
        expecting a structure like
        ```
        {'keywords': [['presto wrapper', 0.0020211625251083634],
        ['model access', 0.0020211625251083634],
        ['test', 0.04491197687864554],
        ['wrapper for model', 0.04702391380834952]]}}
        ```
        """
        keywords = result["keywords"]
        # give back keyword,score pairs to be added to item
        return keywords
