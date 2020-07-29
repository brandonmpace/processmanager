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

"""Module for globals that are only relevant in the main process"""

import concurrent.futures
import logging
import multiprocessing
import queue
import threading

from typing import Dict, List, Optional
from . import globalvars
from . import validation


logger = logging.getLogger(__name__)

# Whether or not we should handle work request in the main process in case worker processes fail to start
fail_open = True

# Set to True after main process confirms that the worker processes are operational
loaded = False

# Set after main process confirms that the worker processes are operational
loaded_event = threading.Event()

# a multiprocessing manager allows simple creation of shared synchronized objects
manager: Optional[multiprocessing.Manager] = None

# Queue used for notification requests from other parts of the main process
notification_queue: Optional[queue.Queue] = None

# Queues used by the main process to send notifications to each worker process
notification_queues: List[multiprocessing.Queue] = []

# Whether or not offload to worker processed is enabled
offload_enabled = False

# Set to True if disable_offload is called, and will prvent changing offload_enabled to True automatically
offload_force_disabled = False

# Thread pool that handles offloading of work requests and processing of results in the main process
offload_handler: Optional[concurrent.futures.ThreadPoolExecutor] = None

# A list of all worker process instances
process_list: List[multiprocessing.Process] = []

# A map of PID to worker process instance
process_map: Dict[int, multiprocessing.Process] = {}

# Indicates that start_workers has been called
start_called = False

# Indicates that stop has been called
stop_called = False

# Queue for functions in the main process to submit work requests
submission_queue: Optional[queue.Queue] = None


def disable_fail_open():
    """Disable handling work in the main process if child workers fail to initialize.

    By default, if there is an issue with worker processes work will be handled in the main process.
    This changes that behavior to raise an exception instead.
    """
    global fail_open
    with globalvars.local_lock:
        fail_open = False


# Called when NotificationSender dies. Also useful for testing resiliency.
def disable_offload(sticky: bool = True):
    """Disable submissions to worker processes.

    If :func:`disable_fail_open` was not called, new submissions will be handled in the main process.

    :param sticky: (optional) If True, then offload will not be automatically enabled by this package.
    :type sticky: bool
    """
    validation.require_main_context()
    global offload_enabled, offload_force_disabled
    with globalvars.local_lock:
        if sticky:
            offload_force_disabled = True
        offload_enabled = False


def enable_offload(force: bool = True):
    """Enable submissions to worker processes.

    Called automatically with `force=False` when all worker processes have been confirmed to be operational.

    :param force: (optional) When True, will override a previous :func:`disable_offload` call, even if `sticky` was True
    :type force: bool
    """
    validation.require_main_context()
    global offload_enabled, offload_force_disabled
    with globalvars.local_lock:
        if (not force) and offload_force_disabled:
            logger.warning("Will not enable offload to worker processes because it was forced to disabled state")
        else:
            offload_enabled = True
            offload_force_disabled = False


def initialize_globals(process_count: int):
    """Internal function to prepare the main process globals used in this package."""
    validation.require_main_context()
    if not isinstance(process_count, int):
        raise TypeError(f"Expected int, got {type(process_count)}")
    with globalvars.local_lock:
        if globalvars.initialized:
            raise RuntimeError("duplicate call to initialize_globals")

        global manager, notification_queue, offload_handler, submission_queue
        manager = multiprocessing.Manager()

        shared_dict = manager.dict()
        shared_dict["cross_process_lock"] = manager.RLock()
        shared_dict["process_count"] = process_count
        shared_dict["queues_ready_event"] = manager.Event()
        shared_dict["test_success"] = manager.list()

        notification_queue = queue.Queue(process_count + 1)

        offload_handler = concurrent.futures.ThreadPoolExecutor(
            max_workers=process_count + 1, thread_name_prefix="offload"
        )

        submission_queue = queue.Queue(process_count + 2)

        work_queue = multiprocessing.Queue(process_count + 2)

        globalvars.set_globals(shared_dict, work_queue)


def set_loaded():
    """Internal function that is called when all worker processes have been confirmed to be operational."""
    validation.require_main_context()
    with globalvars.local_lock:
        global loaded
        loaded = True
        loaded_event.set()


def set_start_called():
    """Internal function that is called when start_workers is called"""
    validation.require_main_context()
    with globalvars.local_lock:
        global start_called
        start_called = True


def set_stop_called():
    """Internal function that is called when termination.stop is called"""
    validation.require_main_context()
    with globalvars.local_lock:
        global stop_called
        stop_called = True
