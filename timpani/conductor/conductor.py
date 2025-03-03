from timpani.app_cfg import TimpaniAppCfg
import schedule
import time
import threading

import timpani.util.timpani_logger

logging = timpani.util.timpani_logger.get_logger()


def run_scheduler(interval=1):
    """
    Creates a scheduling thread and returns an event hook that can be used to stop it
    """
    event_hook = threading.Event()

    class ScheduleThread(threading.Thread):
        @classmethod
        def run(cls):
            while not event_hook.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    # the thread must be demonic so it will shutdown when process finishes
    continuous_thread.daemon = True
    continuous_thread.start()
    return event_hook


class ProcessConductor(object):
    """
    This is a placeholder for the schedular/heartbeat task that will monitor and trigger
    processes that are not triggered by external tasks
    * resuming import processes and workflows that have failed or stopped by redeploy
    * periodically triggering the cluster updating process
    * running the (daily) jobs to remove old content
    * triggering updates to batch process that need to flush every few minutes
    TODO: need shutdown process to kill this
    """

    cfg = TimpaniAppCfg()

    def __init__(self, orchestrator=None) -> None:
        self.event_hook = None
        pass

    def register_scheduled_function(self, job_function, interval_seconds):
        """Record a schedule and an action it should trigger"""
        schedule.every(interval_seconds).seconds.do(job_function)
        logging.info(
            f"Scheduled {job_function} to run every {interval_seconds} seconds"
        )

    def start_schedular(self):
        """
        Start monitoring all the things that need monitoring, checking
        every second to see if they need triggering
        @return cease_continuous_run: threading.Event which can
         be set to top.
        """
        # get the list of active workspaces to pull paramters and timing

        # wait some reasonable interval to not start everything off at once

        # query for in-progress jobs that need to be restarted
        # https://meedan.atlassian.net/browse/CV2-4249

        # kick of a thread that that will keep checking scheduled jobs
        self.event_hook = run_scheduler()

    def stop_schedular(self):
        if self.event_hook is not None:
            self.event_hook.set()
