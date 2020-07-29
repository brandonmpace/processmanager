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

"""Module for globals that are only relevant in worker processes"""

import multiprocessing

from typing import Optional
from . import globalvars
from . import validation


# Checked for state management. Process will exit when False
keep_running = True

# A queue to receive notifications from the main process. (receive ONLY - DO NOT .put)
notification_queue: Optional[multiprocessing.Queue] = None


def set_worker_globals(worker_process):
    """Set globals from a WorkerProcess instance"""
    validation.require_worker_context()
    with globalvars.local_lock:
        global notification_queue

        notification_queue = worker_process.notification_queue
        shared_dict = worker_process.global_dictionary
        work_queue = worker_process.work_input_queue

        globalvars.set_globals(shared_dict, work_queue)
        # Automatically propagate multiprocessing package logger to root in worker processes to simplify log management.
        # This makes it so that debugging can be performed with the root logger in worker processes
        multiprocessing.get_logger().propagate = True


def stop_running():
    """Called from notification handler thread when a stop notification is received"""
    validation.require_worker_context()
    global keep_running
    keep_running = False
