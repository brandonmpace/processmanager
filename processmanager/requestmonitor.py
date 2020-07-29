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

"""Module for handling work requests with a single thread"""

import logging
import threading

from . import globalvars
from . import mainprocessglobals
from . import workrequestclasses
from typing import Optional


logger = logging.getLogger(__name__)


monitor_thread: Optional['WorkRequestMonitor'] = None


class WorkRequestMonitor(threading.Thread):
    """A dedicated thread to monitor for work requests and place them in the cross-process work queue."""
    def run(self):
        logger.debug("work request monitor starting")
        while True:
            try:
                new_request: Optional[workrequestclasses.WorkRequest] = mainprocessglobals.submission_queue.get()
                mainprocessglobals.submission_queue.task_done()
                if new_request is None:
                    break  # sentinel for stop
                elif new_request.cancelled:
                    logger.info(f"Request was cancelled: {new_request}")
                elif globalvars.current_state.go:
                    # TODO: consider using a timeout
                    globalvars.work_queue.put(new_request)
                else:
                    logger.info(f"Global no-go, request dropped: {new_request}")
                    new_request.cancel()
            except Exception:
                logger.exception("Exception in work request monitoring thread")
                raise
        logger.info("work request monitor exiting main loop")


def start():
    global monitor_thread
    monitor_thread = WorkRequestMonitor(daemon=True, name="RequestMonitor")
    monitor_thread.start()


def stop():
    mainprocessglobals.submission_queue.put(None)
