import sentry_sdk
from timpani.content_store.content_store_interface import ContentStoreInterface
from timpani.processing_sequences.workflow import Workflow

from timpani.processing_sequences.test_workflow import TestWorkflow
from timpani.processing_sequences.noop_workflow import NoopWorkflow
from timpani.processing_sequences.default_workflow import DefaultWorkflow

from timpani.processing_sequences.meedan_workflow import MeedanWorkflow

from timpani.processing_sequences.meedan_tse_workflow import MeedanTSEWorkflow

from timpani.processing_sequences.classy_india_election_workflow import (
    ClassyIndiaElectionWorkflow,
)
from timpani.processing_sequences.meedan_aapi_workflow import (
    MeedanAAPIWorkflow,
)


import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


class WorkflowManager(object):
    """
    Class for managing access to workspace configurations (either from disk, db, or an internal repository)
    """

    REGISTRED_WORKFLOWS = [
        DefaultWorkflow,
        MeedanWorkflow,
        MeedanTSEWorkflow,
        TestWorkflow,
        NoopWorkflow,
        MeedanAAPIWorkflow,
        ClassyIndiaElectionWorkflow,
    ]
    loaded_workflows = {}

    def __init__(self, content_store: ContentStoreInterface) -> None:
        """
        Instantiate the workflows it knows about (reporting errors)
        and create a lookup dictionary by slug.
        """
        logging.info("Loading workflows")
        for cls in self.REGISTRED_WORKFLOWS:
            try:
                # instantiate the workflow with reference to db
                workflow = cls(content_store=content_store)
                slug = workflow.get_name()
                # make sure we don't have two workspaces with same id slug
                if slug in self.loaded_workflows:
                    logging.warning(
                        f"Overwriting workflow id {slug} with workflow {workflow}"
                    )
                self.loaded_workflows[slug] = workflow
                logging.debug(f"loaded workflow:{workflow}")
            except Exception as e:
                err = f"Unable to load processing workflow {cls} due to error: {e}"
                logging.exception(err)
                sentry_sdk.capture_exception(e)

    def get_workflow(self, workflow_id: str) -> Workflow:
        """
        Return content processing sequence corresponding to workspace_id
        """
        assert (
            workflow_id in self.loaded_workflows
        ), f"workflow_id '{workflow_id}' does not match any known content processing workflows."
        return self.loaded_workflows[workflow_id]
