from timpani.model_service.alegre_wrapper_service import AlegreWrapperService
from timpani.content_store.content_item import ContentItem


class MeansTokensAlegreVectorizationModelService(AlegreWrapperService):
    """
    Wrapper for the text vectorization model service as implemented in alegre,
    this specific implementation uses the Means Tokens model
    TODO: this should be replaced by code that talks to specific model service
    hosted by presto
    TODO: in the short term, could we talk directly to the means-token's model host?
    """

    MODEL_KEY = "xlm-r-bert-base-nli-stsb-mean-tokens"

    def vectorize_content_item(self, item: ContentItem, target_state: str):
        # TODO: batching
        self._do_alegre_vectorization(
            self.MODEL_KEY,
            content_item_id=item.content_item_id,
            content_text=item.content,
            workspace_id=item.workspace_id,
        )
        # TODO:  call the callback url on conductor that would in theory
        # be called by Presto or the model service to update state
        self._do_state_callback(item.content_item_id, target_state)
