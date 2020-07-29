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

"""Module for functions that other packages can use to request work"""

import concurrent.futures
import logging
import multiprocessing

from typing import Any, Callable, Mapping, Optional, Tuple, Type
from . import exceptions
from . import globalvars
from . import mainprocessglobals
from . import validation
from . import workrequestclasses


logger = logging.getLogger(__name__)


def submit(
        func: Callable,
        args: Tuple = (),
        kwargs: Optional[Mapping[str, Any]] = None,
        handler_class: Type[workrequestclasses.ResultHandler] = workrequestclasses.ResultHandler,
        is_generator: bool = False
) -> concurrent.futures.Future:
    """Submit a request to run a function in a worker process.

    :param func: function to run
    :type func: callable
    :param args: (optional) args to pass to func. Single argument needs a trailing comma. e.g. (5,)
    :type args: tuple
    :param kwargs: (optional) kwargs to pass to func
    :type kwargs: dict
    :param handler_class: type of ResultHandler to use (may be a subclass of :class:`ResultHandler`) (not an instance)
    :param is_generator: When True indicates that func is a generator and will yield results
    :type is_generator: bool
    :return: a Future object that will get the result of the work
    :rtype: concurrent.futures.Future

    :raises: :exc:`ChildProcessError` if there is an issue with workers and :func:`disable_fail_open` was used

    .. note:: The Future may get :exc:`processmanager.CancelledError` if :attr:`processmanager.globalvars.current_state`
        has no_go == True when it is handled

    Example::

        # Say we had a function:
        def some_heavy_func(first, second, a=0, b=0):
            # some CPU-intensive operation done here

        # The work request submission could look something like this:
        new_future = processmanager.submit(some_heavy_func, args=(1, 2), kwargs={"a": 0, "b": 1})

        # Then later access the result:
        new_result = new_future.result()
    """
    validation.require_main_context()
    if not mainprocessglobals.start_called:
        raise RuntimeError("Work request submission before start_workers was called")
    elif mainprocessglobals.stop_called:
        raise RuntimeError("stop was called. Unable to handle new submissions.")

    if globalvars.current_state.no_go:
        raise exceptions.CancelledError("Global no_go is True, request was cancelled")

    if kwargs is None:
        kwargs = {}

    if mainprocessglobals.loaded:  # Worker processes are known to have started successfully
        should_offload = mainprocessglobals.offload_enabled
        logger.debug(f"Using should_offload value of {should_offload}")
    elif mainprocessglobals.fail_open:  # If not loaded, see if we should handle requests in the main process
        logger.warning("Worker processes are not loaded and fail_open is True. Will handle work in main process.")
        should_offload = False
    else:
        raise ChildProcessError("In broken state and unable to handle work")

    # TODO: Implement a check that all processes are still alive to automatically disable offload
    #  (maybe check when work queue is full?) e.g. using process.is_alive()

    if should_offload:  # Handle in worker process
        main_pipe, worker_pipe = multiprocessing.Pipe(duplex=False)
    else:  # Handle in the main process
        main_pipe = worker_pipe = None

    work_request = workrequestclasses.WorkRequest(is_generator, worker_pipe, func, *args, **kwargs)
    work_handler = handler_class(work_request, main_pipe)

    if should_offload:
        _submit_request(work_request)

    return mainprocessglobals.offload_handler.submit(work_handler.run)


def _submit_request(new_request):
    """Internal function to add a request to the submission queue"""
    # TODO: consider using a timeout
    mainprocessglobals.submission_queue.put(new_request)
