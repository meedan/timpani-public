from timpani.model_service.alegre_wrapper_service import AlegreWrapperService
from timpani.content_store.content_item import ContentItem


class ParaphraseMultilingualAlegreVectorizationModelService(AlegreWrapperService):
    """
    Wrapper for the text vectorization model service as implemented in alegre
    https://huggingface.co/sentence-transformers/paraphrase-multilingual-mpnet-base-v2
    TODO: this should be replaced by code that talks to specific model service
    hosted by presto
    """

    MODEL_KEY = "paraphrase-multilingual-mpnet-base-v2"

    def vectorize_content_item(self, item: ContentItem, target_state: str):
        # TODO: this should be updated to support batching when in alegre/presto
        self._do_alegre_vectorization(
            self.MODEL_KEY,
            content_item_id=item.content_item_id,
            content_text=item.content,
            workspace_id=item.workspace_id,
        )
        # TODO:  call the callback url on conductor that would in theory
        # be called by Presto or the model service to update state
        self._do_state_callback(item.content_item_id, target_state)
