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

"""
The processmanager package provides a way to start a pool of worker processes and obtain results of work from them.

It allows offloading CPU-intensive work to other processor cores to improve overall performance.
"""


__author__ = "Brandon M. Pace"
__copyright__ = "Copyright 2019-2020 Brandon M. Pace"
__license__ = "GNU LGPL 3+"
__maintainer__ = "Brandon M. Pace"
__status__ = "Development"
__version__ = "0.1.1"


import logging
import multiprocessing

# NOTE: functions imported here are considered the 'public' API
from .exceptions import WorkError, CancelledError
from .helpers import current_process_count, processes_started, wait_for_complete_load, wait_for_process_start
from .initialization import prepare_globals, start_workers
from .mainprocessglobals import disable_fail_open, disable_offload, enable_offload
from .notifications import add_custom_notification, enqueue_notification, update_log_level, update_state_value
from .termination import stop
from .workers import add_init_func
from .workrequests import submit
from .workrequestclasses import ResultHandler

# globalvars is present here only to allow for use of the cross_process_lock from within custom notification handlers
# That is useful when there is some resource or action that needs to be restricted to one process at a time.
# It is also to allow access to current_state, so functions that are offloaded can check for go/no_go condition.
# That allows faster cancellation of work.
from . import globalvars
from .globalvars import current_state


logger = logging.getLogger(__name__)

# Note: In main process, the multiprocessing package logger is not set to propagate (by default)
# In worker processes, propagation gets enabled by this package to allow it to 'trickle down' into the root logger,
# in order to simplify logging in worker processes.
# This variable is for reference to simplify managing the log level of that logger.
multiprocessing_logger = multiprocessing.get_logger()
