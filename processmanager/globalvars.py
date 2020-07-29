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

"""Module for globals that are relevant in all processes"""

import multiprocessing
import threading

from typing import Optional


class StateValue:
    """This can be used as a proxy for ThreadManager state"""
    def __init__(self, initial_value: bool = True):
        self.go = initial_value

    @property
    def no_go(self):
        return not self.go

    def update(self, new_value: bool):
        if isinstance(new_value, bool):
            self.go = new_value
        else:
            raise TypeError(f"Expected bool, got {type(new_value)}")

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.go)


cross_process_lock: Optional[multiprocessing.RLock] = None
"""A re-entrant lock (RLock) that works across all processes.

This can be useful if you have actions that should be performed in only one process at a time

Always use context-manager style::

    with processmanager.globalvars.cross_process_lock:
        some_func()

.. warning:: only available after calling :func:`processmanager.prepare_globals` or :func:`processmanager.start_workers`
"""


current_state = StateValue()
"""Used to determine if processing should continue.

.. note:: This is also accessible as `processmanager.current_state`

A proxy for threadmanager go/no_go, updated via notifications in worker processes and callbacks in the main process.

To add relevant callbacks when using threadmanager::

    tm = threadmanager.ThreadManager("example")
    tm.add_idle_callback(processmanager.update_state_value, True)
    tm.add_stop_callback(processmanager.update_state_value, False)

This can be used to abort execution from within the functions you submit to processmanager.

You can check for no_go to see if you should stop processing, then either raise an exception or return early::

    def some_work_func():
        if processmanager.current_state.no_go:
            # Either raise the below exception or return early
            raise processmanager.CancelledError("Aborting work due to stop request")

Or check the go property to determine if you should continue processing::

    def some_work_func():
        if processmanager.current_state.go:
            # perform your offloaded work here
        else:
            # Either raise the below exception or return early
            raise processmanager.CancelledError("Aborting work due to stop request")
"""

# Set to True after globals are populated
initialized = False

# Gets set after globals are populated
initialized_event = threading.Event()

# A lock for the current process
local_lock = threading.RLock()

# Amount of time the main process waits for workers to ACK, and how long worker processes wait for initial test message.
initialization_timeout = 20

# Interval (seconds) at which the main process will send keep-alive notifications if no other notification has been sent
keep_alive_interval = 30

# Amount of time worker processes will wait for notifications from the main process.
# If the timeout is reached, the worker process will exit as the main process is probably gone.
keep_alive_timeout = keep_alive_interval * 2

# A fully synchronized Event that gets set once all worker processes confirm receipt of initial test notification
# The NotificationMonitor thread in the last worker process to ACK is the one that calls Event.set()
queues_ready_event: Optional[multiprocessing.Event] = None

# A dictionary that is synchronized across all processes
shared_dict: Optional[dict] = None

# Queue for the main process to send work items to all worker processes, items are sent by a WorkRequestMonitor thread
work_queue: Optional[multiprocessing.Queue] = None


def set_globals(global_dict, shared_work_queue):
    """Set assignments for globals in this module.

    Internal function that is called when initialization is happening. Relevant in all processes.
    """
    with local_lock:
        global cross_process_lock, initialized, queues_ready_event, shared_dict, work_queue
        cross_process_lock = global_dict["cross_process_lock"]
        queues_ready_event = global_dict["queues_ready_event"]
        shared_dict = global_dict
        work_queue = shared_work_queue
        initialized = True
        initialized_event.set()
