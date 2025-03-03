import unittest
from datetime import datetime
from timpani.app_cfg import TimpaniAppCfg
from timpani.content_store.content_store import ContentStore
from timpani.content_store.content_store_manager import ContentStoreManager

# from timpani.content_store.content_store_obj import ContentStoreObject
from timpani.content_store.item_state_model import ContentItemState
from timpani.content_store.content_item import ContentItem
from timpani.processing_sequences.default_workflow import DefaultContentItemState
from timpani.processing_sequences.test_workflow import TestContentItemState
from timpani.conductor.orchestrator import Orchestrator


class TestForContentItemState(unittest.TestCase):
    """
    Make sure we can create content items with
    appropriate state model, save to db, and update
    """

    cfg = TimpaniAppCfg()

    @classmethod
    def setUpClass(self):
        """
        Should only be called once during test run because
        it will reset database and cause deadlocks
        """

        # define all the new tables
        # don't run in prod env?
        assert self.cfg.deploy_env_label not in [
            "live",
            "qa",
        ], "Exiting test to avoid modifying live or qa database"
        print("\nDrop and recreating all tables for item state test run\n")
        manager = ContentStoreManager()
        manager.destroy_content_store()
        manager.setup_admin_and_content_store()
        # ContentStoreObject.metadata.drop_all(bind=engine)
        # ContentStoreObject.metadata.create_all(bind=engine)

        # create a content store
        self.store = ContentStore()
        self.store.init_db_engine()

    # TODO: confirm state validation

    def test_item_init_and_transition(self):
        item = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388abz",
            raw_content="Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        )
        state = ContentItemState()
        # should inherit undefined and add 3 more
        assert set(state.valid_states) == set(
            ["undefined", "ready", "failed", "completed"]
        )
        item = self.store.initialize_item(item, state)
        assert item is not None
        assert item.content_item_state_id is not None
        state = self.store.refresh_object(state)
        # should have the same states after reload
        # should inherit undefined and add 3 more
        assert set(state.valid_states) == set(
            ["undefined", "ready", "failed", "completed"]
        ), f" valid states: {state.valid_states}"
        assert item.content_item_state_id == state.state_id
        assert state.current_state == ContentItemState.STATE_UNDEFINED
        assert state.transition_num == 0
        assert state.transition_start is not None
        assert state.transition_end is not None
        assert state.completed_timestamp is None
        # starting state transition should increment the transition counter
        new_state = self.store.start_transition_to_state(item, state.STATE_READY)
        assert new_state.transition_num == 1

        new_state = self.store.transition_item_state(item, state.STATE_READY)
        assert new_state.current_state == state.STATE_READY
        assert new_state.transition_num == 1
        new_state = self.store.refresh_object(new_state)
        assert new_state.current_state == state.STATE_READY

        # changing to invalid state should cause error
        self.assertRaises(
            AssertionError, self.store.transition_item_state, item, "BAD_STATE"
        )

        # changing to a valid but inappropriate state should cause error
        self.assertRaises(
            AssertionError,
            self.store.transition_item_state,
            item,
            ContentItemState.STATE_UNDEFINED,
        )

    def test_multiple_inheritance(self):
        """
        Make sure we don't bleed state betwen classes
        """

        default_state = DefaultContentItemState()
        # should inherit undefined, ready, failed, completed, and add vectorized and clustered
        assert set(default_state.valid_states) == set(
            ["undefined", "ready", "failed", "completed", "vectorized", "clustered"]
        )

        test_state = TestContentItemState()
        # should inherit undefined, ready, failed, completed, and add placedholder and delayed
        assert set(test_state.valid_states) == set(
            ["undefined", "ready", "failed", "completed", "placeholder", "delayed"]
        )

        basic_state = ContentItemState()
        # should inherit undefined and add 3 more
        assert set(basic_state.valid_states) == set(
            ["undefined", "ready", "failed", "completed"]
        )

    def test_item_default_life_cycle(self):
        """
        Check that all the operations for happy path work
        """
        # create and store item
        item = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388abc",
            raw_content="Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        )
        state = DefaultContentItemState()
        assert set(state.valid_states) == set(
            ["undefined", "ready", "failed", "completed", "vectorized", "clustered"]
        ), f"valid states: {state.valid_states}"
        item = self.store.initialize_item(item, state)
        self.store.start_transition_to_state(item, DefaultContentItemState.STATE_READY)
        self.store.transition_item_state(item, DefaultContentItemState.STATE_READY)
        item = self.store.refresh_object(item)

        # get the items that are ready
        ready_items = list(self.store.get_items_in_progress())
        assert item in ready_items, f"the items are {ready_items}"

        # next step, vectoriziation!
        for item in ready_items:
            start = datetime.utcnow()
            # Starting a transition and submitting a job
            state = self.store.start_transition_to_state(
                item, DefaultContentItemState.STATE_VECTORIZED
            )

            # this is where we submit to vectorization service ...

            # confirm start time timestamp updates
            assert state.transition_start > start
            assert state.transition_end < state.transition_start

        # yay, we got callbacks from vectorization, update them in the db
        for item in ready_items:
            # pretend we sent it off to get vectored, but for some reason that also changed the text
            new_content = "If you have the next $1,200 share to Protect your tax money support to America Great"
            item.content = new_content
            end = datetime.utcnow()
            item = self.store.update_item(
                item, DefaultContentItemState.STATE_VECTORIZED
            )
            # confirm end time timestamp updates
            state = self.store.refresh_object(state)
            assert (
                state.transition_start < state.transition_end
            ), f"start {state.transition_start} end {state.transition_end}"

            assert state.transition_end > end
            assert item.content == new_content

        # pretend we applied the cluster logic
        ready_items = list(self.store.get_items_in_progress())
        for item in ready_items:
            # Starting a transition and submitting a job

            self.store.start_transition_to_state(
                item, DefaultContentItemState.STATE_CLUSTERED
            )
            # now we make a clauster
            self.store.cluster_items(item)
            self.store.transition_item_state(
                item, DefaultContentItemState.STATE_CLUSTERED
            )

        # indicate that we are done
        ready_items = list(self.store.get_items_in_progress())
        for item in ready_items:
            # Starting a transition and submitting a job
            self.store.start_transition_to_state(
                item, DefaultContentItemState.STATE_COMPLETED
            )
            self.store.transition_item_state(
                item, DefaultContentItemState.STATE_COMPLETED
            )
            # check that completed timestep is set
            state = self.store.refresh_object(state)
            assert state.completed_timestamp is not None
            assert state.transition_num == 4, f"transition_num {state.transition_num}"
            assert state.current_state == DefaultContentItemState.STATE_COMPLETED

        # there should be no more ready items
        ready_items = list(self.store.get_items_in_progress())
        assert len(ready_items) == 0, f"ready items {ready_items}"

        # ... after some time passes, we delete the item
        # TODO: store.get_items_ready_for_delete()?
        self.store.delete_item(item)

    def test_state_class_polymorphism(self):
        """
        Make sure that if we create a more specific class,
        we get it back from the database
        """
        item = ContentItem(
            date_id=19000101,
            run_id="run_1c43908277e34803ba7eea51b9054219",
            workspace_id="meedan_test",
            source_id="test_source",
            query_id="test_query_id",
            raw_created_at=datetime.strptime(
                "2023-06-09 10:45:34.715998", "%Y-%m-%d %H:%M:%S.%f"
            ),
            raw_content_id="129839388lmno",
            raw_content="Nếu có $ 1,200 tiếp theo share để Bảo vệ tiền thuế support to America Great",
        )

        # create with a state class that has additional states and transitions defined
        state = DefaultContentItemState()
        item = self.store.initialize_item(item, state)
        self.store.transition_item_state(item, state.STATE_READY)

        assert self.store.validate_transition(item, state.STATE_VECTORIZED) is True

        # item_state = self.store.transition_item_state(item, state.STATE_VECTORIZED)
        item_state = Orchestrator(self.store).update_content_item_state(
            item.content_item_id, state.STATE_VECTORIZED
        )
        self.store.delete_item(item)

        assert (
            type(item_state).__name__ == "DefaultContentItemState"
        ), f"state type is {type(item_state).__name__}"
