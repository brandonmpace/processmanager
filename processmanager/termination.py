"""Module for termination from the main process (stopping worker processes)"""

import logging

from . import globalvars
from . import notifications
from . import mainprocessglobals
from . import requestmonitor
from . import validation


logger = logging.getLogger(__name__)


def stop():
    """Stop worker processes.

    This will send shutdown notifications to all worker processes, and they will exit their main loops.

    It is recommended to call this before exiting your program. (e.g. by adding to an :mod:`atexit` handler)
    """
    validation.require_main_context()
    mainprocessglobals.set_stop_called()

    if globalvars.initialized:
        logger.info("stop called")
    else:
        logger.info("stop called, but processmanager was not initialized")
        return

    if mainprocessglobals.offload_enabled:
        mainprocessglobals.disable_offload()

    if mainprocessglobals.loaded:
        notifications.send_shutdown_notification()

    requestmonitor.stop()

    for _ in range(globalvars.shared_dict["process_count"]):
        globalvars.work_queue.put(None)

    mainprocessglobals.offload_handler.shutdown()
