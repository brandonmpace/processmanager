# -*- coding: UTF-8 -*-
# Copyright (C) 2019-2020 Brandon M. Pace
#
# This file is part of processmanager
#
# processmanager is free software: you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# processmanager is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with processmanager.
# If not, see <https://www.gnu.org/licenses/>.

"""Module for initialization from the main process"""

import logging

from . import globalvars
from . import helpers
from . import notifications
from . import mainprocessglobals
from . import requestmonitor
from . import validation
from . import workers


logger = logging.getLogger(__name__)


def prepare_globals(process_count: int = 0):
    """Initialize globals without starting any processes.

    It is not required to call this, as :func:`processmanager.start_workers` will call it for you.

    :param process_count: Number of worker processes to request.
    :type process_count: int
    """
    validation.require_main_context()
    final_process_count = helpers.get_best_process_count(process_count)
    mainprocessglobals.initialize_globals(final_process_count)


def start_workers(process_count: int = 0):
    """Start the worker processes.

    This should be called once per program run.

    The workers will run until :func:`processmanager.stop` is called or the program exits.

    :param process_count: Number of worker processes to request.
    :type process_count: int
    """
    validation.require_main_context()
    with globalvars.local_lock:
        if mainprocessglobals.start_called:
            raise RuntimeError("start_workers has already been called!")
        else:
            mainprocessglobals.set_start_called()

    if globalvars.initialized:
        final_process_count = globalvars.shared_dict["process_count"]
        logger.debug(f"globals already initialized, using final process count of {final_process_count}")
    else:
        prepare_globals(process_count)
        final_process_count = globalvars.shared_dict["process_count"]

    requestmonitor.start()

    for num in range(final_process_count):
        logger.info(f"initializing worker number {num}")
        new_notification_queue = mainprocessglobals.manager.Queue()
        new_process = workers.WorkerProcess(new_notification_queue, num)
        mainprocessglobals.process_list.append(new_process)
        mainprocessglobals.process_map[new_process.ident] = new_process
        mainprocessglobals.notification_queues.append(new_notification_queue)
        new_process.start()
        logger.info(f"Worker {num} PID: {new_process.ident} Alive: {new_process.is_alive()}")

    notifications.start_notification_sender_thread()

    if helpers.wait_for_process_start(globalvars.initialization_timeout - 5):
        logger.info("Process notification queues all registered successfully")
        mainprocessglobals.set_loaded()
        mainprocessglobals.enable_offload(force=False)
    else:
        logger.error("Process notification queues have not been registered yet!")
        if helpers.wait_for_process_start(5):
            logger.info("worker processes became available")
            mainprocessglobals.set_loaded()
            mainprocessglobals.enable_offload(force=False)
        else:
            logger.error("worker processes still unavailable, we are in a broken state!")
