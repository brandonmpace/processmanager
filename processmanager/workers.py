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

"""Module for worker-related items"""

import logging
import multiprocessing
import os
import sys

from typing import Callable, List, Optional
from . import globalvars
from . import mainprocessglobals
from . import notifications
from . import validation
from . import workerglobals
from . import workrequestclasses


logger = logging.getLogger(__name__)


_init_functions: List[workrequestclasses.Runnable] = []


def add_init_func(func: Callable, *args, **kwargs):
    """Add a function to run in each worker process as it starts.

    This can be used to perform actions such as loading configuration from files.

    :param func: function to run in new worker process when it starts
    :param args: args to pass to the function
    :param kwargs: kwargs to pass to the function

    .. note:: This should be called before :func:`processmanager.start_workers` if used.
    .. seealso:: :attr:`processmanager.globalvars.cross_process_lock` for restricting actions to one process at a time.
    """
    validation.require_main_context()
    with globalvars.local_lock:
        if mainprocessglobals.start_called:
            raise RuntimeError("Worker init function registration must be performed before starting workers")
        new_runnable = workrequestclasses.Runnable(func, *args, **kwargs)
        if new_runnable in _init_functions:
            raise RuntimeError(f"Init function already added: {new_runnable}")
        else:
            _init_functions.append(new_runnable)


class WorkerProcess(multiprocessing.Process):
    def __init__(self, notification_queue, worker_id):
        self.global_dictionary = globalvars.shared_dict
        self.init_functions = _init_functions
        self.notification_queue = notification_queue
        self.notification_map = notifications.notification_map
        self.work_input_queue = globalvars.work_queue
        self.worker_id = worker_id
        super().__init__(target=main_loop, name=f"WorkerProcess{worker_id}", daemon=True)

    def run(self) -> None:
        workerglobals.set_worker_globals(self)
        notifications.notification_map.update(self.notification_map)
        notifications.start_notification_monitor_thread(self.worker_id)
        logger.debug("started notification monitoring thread")
        try:
            globalvars.queues_ready_event.wait(globalvars.keep_alive_timeout + 1)
        except BrokenPipeError:
            logger.error("broken pipe, exiting. It is likely that the main process is gone..")
            sys.exit()
        with globalvars.cross_process_lock:
            for runnable in self.init_functions:
                try:
                    runnable.run()
                except Exception:
                    logger.exception("Exception in custom init function")
        try:
            super().run()
        except Exception:
            logger.exception("Exception in WorkerProcess")


def main_loop():
    """The main loop for worker processes"""
    logger.debug("process main loop starting")

    while workerglobals.keep_running:
        try:
            new_work_item: Optional[workrequestclasses.WorkRequest] = globalvars.work_queue.get()
            if new_work_item is None:
                break  # None is the sentinel for shutdown trigger
            logger.debug(f"{os.getpid()} got {type(new_work_item)}")
            if new_work_item.cancelled:
                logger.error(f"Got cancelled work request: {new_work_item}")
            elif globalvars.current_state.no_go:
                logger.info(f"Global no_go is True, will cancel work request {new_work_item}")
                new_work_item.cancel()
            else:
                new_work_item.run()
            if not notifications.notification_monitor.is_alive():
                logger.error("NotificationMonitor thread is not alive, will exit.")
                break
        except Exception:
            logger.exception("Exception in worker process main loop")
    logger.info("leaving main loop, will exit")
