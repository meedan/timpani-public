import time
import unittest
from datetime import datetime

# from timpani.model_service.means_tokens_vectorization_alegre_wrapper import (
#    MeansTokensAlegreVectorizationModelService as AlegreService,
# )

from timpani.model_service.paraphrase_multilingual_vectorization_alegre_wrapper import (
    ParaphraseMultilingualAlegreVectorizationModelService as AlegreService,
)
from timpani.content_store.content_item import ContentItem
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.processing_sequences.default_workflow import DefaultContentItemState
from timpani.vector_store.alegre_store_wrapper import AlegreVectorStoreService
from timpani.app_cfg import TimpaniAppCfg


class TestAlegreWrappers(unittest.TestCase):
    """
    NOTE: some of this is a little sketchy because we a pretending Alegre works via callbacks and it doesn't
    TODO: How do we run this test in CI or if alegre offline? When starting Alegre locally to run tests, need
    to run alegre and the means tokens model FROM THE CHECK DOCKER COMPOSE
        cd /check  docker compose up alegre paraphrase_multilingual_mpnet_base_v2
    """

    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self):
        # define all the new tables
        # don't run in prod env?
        assert self.cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()

        # create a content store
        self.content_store = ContentStore()
        self.content_store.init_db_engine()

        # ContentStoreObject.metadata.drop_all(bind=engine)
        # ContentStoreObject.metadata.create_all(bind=engine)

        # create a test item and cache it to the db
        self.item1 = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test_alegre_wrapper",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388abc",
            raw_content="This is a test of the Alegre wrapper for vectorization",
        )

        self.item2 = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test_alegre_wrapper",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388def",
            raw_content="This is another test item of the Alegre wrapper for vectorization",
        )

        self.state1 = DefaultContentItemState()
        self.item1 = self.content_store.initialize_item(self.item1, self.state1)
        self.state2 = DefaultContentItemState()
        self.item2 = self.content_store.initialize_item(self.item2, self.state2)

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_alegre_vectorization(self):
        """
        Confirm we can submit some text as vector and trigger appropriate callbacks
        """

        vector_model = AlegreService()  # MeansTokensAlegreVectorizationModelService()
        self.content_store.transition_item_state(
            self.item1, DefaultContentItemState.STATE_READY
        )
        state = self.content_store.refresh_object(self.state1)
        assert (
            state.current_state == DefaultContentItemState.STATE_READY
        ), f"current state was {state.current_state}"

        self.content_store.start_transition_to_state(
            self.item1, DefaultContentItemState.STATE_VECTORIZED
        )
        vector_model.vectorize_content_item(
            self.item1, target_state=DefaultContentItemState.STATE_VECTORIZED
        )
        #  confirm that the state was updated by the callback
        # TODO: delay?
        state = self.content_store.refresh_object(self.state1)
        assert (
            state.current_state == DefaultContentItemState.STATE_VECTORIZED
        ), f"current state was {state.current_state}"

    # don't run these tests in the CI environment because it doesn't talk
    # to full aws resources
    @unittest.skipIf(
        TimpaniAppCfg().deploy_env_label in ["test", "live"],
        "integration tests not run in CI test environment",
    )
    def test_alegre_vector_storage(self):
        """
        make sure we can store and retrieve vector via Alegre wrappers
        TODO: use a different item so states don't collide
        """

        vector_service = AlegreVectorStoreService()

        # healthcheck service to make sure it is up
        assert vector_service.healthcheck() is True

        # store the vector
        # this actually uses the text, not the vector
        vector_service.store_vector_for_content_item(self.item1)
        vector_service.store_vector_for_content_item(self.item2)
        time.sleep(5)
        # look up similar items
        similar_ids = vector_service.request_similar_content_item_ids(self.item2)
        assert len(similar_ids) > 0, f"similar ids were {similar_ids}"
        # it should be a score, id pair
        assert similar_ids[0][1] is not None

        # try to delete vector from store
        vector_service.discard_vector_for_content_item(self.item2)
        vector_service.discard_vector_for_content_item(self.item1)
        # TODO: how do we confirm delete
