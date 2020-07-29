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

"""Module for small helper functions"""

import freezehelper
import logging
import os

from typing import Optional
from . import globalvars
from . import mainprocessglobals


logger = logging.getLogger(__name__)


def current_process_count() -> int:
    """Get the number of worker processes that was chosen during initialization"""
    if globalvars.initialized:
        return globalvars.shared_dict.get("process_count", 0)
    else:
        raise RuntimeError("Globals were not initialized yet! Hint: use prepare_globals() or start_workers()")


def get_cpu_count():
    """Get the number of CPUs available to the current process"""
    if freezehelper.is_linux:
        return len(os.sched_getaffinity(0)) or 1
    else:
        return os.cpu_count() or 1


def get_best_process_count(requested_count: int) -> int:
    """Gets the best process count for handling load, with preference being the requested_count value.

    If requested_count is more than the available CPU count, the recommended value is returned.

    :param requested_count: int number (0 means automatic)
    :return: recommended count
    :rtype: int
    """
    if not isinstance(requested_count, int):
        raise TypeError(f"Expected int, got {type(requested_count)}")
    available_cpu_count = get_cpu_count()

    if available_cpu_count == 1:
        cores_to_use = 1
    elif (requested_count == 0) or (requested_count >= available_cpu_count):
        cores_to_use = available_cpu_count - 1
    else:
        cores_to_use = requested_count

    logger.info(
        f"Available CPUs: {available_cpu_count}, requested process count: {requested_count}, decision: {cores_to_use}"
    )
    return cores_to_use


def processes_started() -> bool:
    """Check if the worker processes have been started successfully"""
    if globalvars.queues_ready_event is None:
        return False
    else:
        return globalvars.queues_ready_event.is_set()


def wait_for_complete_load(timeout: Optional[float] = None) -> bool:
    """Wait until submission queues are ready to handle work requests.

    This is especially useful in scripts, as you can delay submissions until everything is ready to handle them.

    :param timeout: (optional) when provided will wait up to this many seconds
    :type timeout: float
    :return: bool True when ready, False if timeout was reached and not yet ready
    """
    if mainprocessglobals.loaded_event.wait(timeout):
        return True
    elif mainprocessglobals.start_called:
        return False
    else:
        raise RuntimeError("called before start_workers was called")


def wait_for_process_start(timeout: Optional[float] = None) -> bool:
    """Wait until all worker processes have started successfully.

    Note that there may return slightly before processes are ready for submissions.

    If you need them ready for submissions, use :func:`wait_for_complete_load` instead.

    :param timeout: (optional) when provided will wait up to twice this many seconds
    :type timeout: float
    :return: bool True when ready, False if timeout was reached and not yet ready
    """
    if globalvars.initialized_event.wait(timeout):
        return globalvars.queues_ready_event.wait(timeout)
    else:
        raise RuntimeError("called before start_workers was called")
