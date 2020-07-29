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

"""Module for classes representing work requests that are sent to worker processes"""

import enum
import logging
import multiprocessing.connection
import threading

from typing import Callable, Optional
from . import exceptions
from . import globalvars


logger = logging.getLogger(__name__)

_counter_lock = threading.Lock()
_request_counter = 0
_request_counter_max = 999999


def _new_request_id() -> int:
    """Internal function to get a new ID for a WorkRequest"""
    with _counter_lock:
        global _request_counter
        _request_counter += 1
        if _request_counter > _request_counter_max:
            _request_counter = 0
        return _request_counter


class WorkState(enum.IntEnum):
    INITIAL = 1
    STARTED = 2
    CANCELLED = 3
    COMPLETED = 4
    ERROR = 5


class ResultHandler:
    """Class that handles results from work requests.

    If the work request is a generator, the results are returned in a list by default.

    This behavior can be changed in subclasses.
    """
    def __init__(
            self, work_request: 'WorkRequest', result_pipe: Optional[multiprocessing.connection.Connection] = None
    ):
        """
        :param work_request: WorkRequest instance that is sent to worker process
        :param result_pipe: (optional) multiprocessing Pipe that is receive-only, for collecting results from worker
        """
        self.result = None
        self.result_pipe = result_pipe
        self.work_request = work_request

    def cancel(self):
        """Cancel work from the main process side

        Can be called from within a :meth:`handle_result` override if needed.
        """
        logger.debug(f"Cancel called in handler: {self}")
        if self.result_pipe and (not self.result_pipe.closed):
            self.result_pipe.close()
        if not self.work_request.cancelled:
            self.work_request.cancel()

    def finalize_result(self):
        """Perform any modifications necessary to the current result just before it gets returned.

        .. note:: This can be over-ridden in subclasses to handle data differently.
        """
        if self.work_request.is_generator and (self.result is None):
            self.result = []

    def handle_result(self, result):
        """Handle a result of the function from the work request

        When the work request is a generator, this will accumulate the results in self.result
        In any case, it can be used to transform self.result before it gets returned.

        .. note:: This can be over-ridden in subclasses to handle data differently.
        """
        if self.work_request.is_generator:
            if self.result is None:
                self.result = []
            self.result.append(result)
        else:
            self.result = result

    def run(self):
        if self.result_pipe:  # handling data piped from worker process
            try:
                initial_item = self.result_pipe.recv()
                if initial_item is WorkState.STARTED:
                    logger.debug(f"Work started for {self.work_request}")
                    # After first successful recv() it is safe to close the worker pipe in the main process.
                    # This makes sure that we get EOFError when relevant.
                    self.work_request.close_pipe()
                elif initial_item is WorkState.CANCELLED:
                    logger.warning(f"Work request was cancelled by worker: {self.work_request}")
                    raise exceptions.CancelledError("Request was cancelled by worker")
                elif initial_item is WorkState.ERROR:
                    msg = f"Error in handling work request {self.work_request}"
                    logger.error(f"Error in handling work request {self.work_request}")
                    raise exceptions.WorkError(msg)
                else:
                    msg = f"Unexpected initial data from worker: {initial_item}"
                    logger.error(msg)
                    raise exceptions.WorkError(msg)

                result = self.result_pipe.recv()
                while not isinstance(result, WorkState):
                    self.handle_result(result)
                    result = self.result_pipe.recv()

                if isinstance(result, WorkState):
                    if result is WorkState.CANCELLED:
                        msg = f"In-progress work request was cancelled by worker. Request: {self.work_request}"
                        logger.warning(msg)
                        raise exceptions.CancelledError(msg)
                    elif result is WorkState.COMPLETED:
                        logger.debug(f"Work completed for {self.work_request}")
                    else:
                        raise exceptions.WorkError(f"Unexpected state from worker: {str(result)}")
            except (BrokenPipeError, EOFError):
                if self.work_request.cancelled:
                    msg = f"Work request was cancelled: {self.work_request}"
                    logger.warning(msg)
                    raise exceptions.CancelledError(msg)
                else:
                    msg = f"Work request was cancelled by closed pipe: {self.work_request}"
                    logger.warning(msg)
                    raise exceptions.CancelledError(msg)
        else:  # handling the request in main process
            if self.work_request.is_generator:
                for result in self.work_request.run():
                    self.handle_result(result)
            else:
                self.handle_result(self.work_request.run())

        self.finalize_result()
        return self.result


class Runnable:
    def __init__(self, func: Callable, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.func(*self.args, **self.kwargs)

    def __hash__(self):
        return hash(f"{self.func}{self.args}{self.kwargs}")

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.func, self.args, self.kwargs)


class WorkRequest:
    """Main class for work requests"""
    immutable_states = (WorkState.CANCELLED, WorkState.COMPLETED, WorkState.ERROR)

    def __init__(
            self,
            is_generator: bool,
            send_pipe: Optional[multiprocessing.connection.Connection],
            func: Callable,
            *args,
            **kwargs
    ):
        """Initialize representation of a work request

        :param is_generator: bool True if the provided function acts as a generator
        :param send_pipe: multiprocessing Pipe that will be used to send results back to the main process
        :param func: Callable function to execute in the worker process
        :param args: args for func
        :param kwargs: kwargs for func
        """
        self._cancelled = False
        self._is_generator = is_generator
        self._runnable = Runnable(func, *args, **kwargs)
        self._send_pipe = send_pipe
        self._state = WorkState.INITIAL
        self._id = _new_request_id()

    def cancel(self):
        if self._cancelled:
            logger.warning(f"redundant call to cancel() for {self}")
        else:
            self._cancelled = True
            self.update_state(WorkState.CANCELLED)
            self.close_pipe()

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def close_pipe(self):
        if self._send_pipe and (not self._send_pipe.closed):
            self._send_pipe.close()

    @property
    def is_generator(self):
        return self._is_generator

    def run(self):
        if self._state is not WorkState.INITIAL:
            raise exceptions.WorkError(f"WorkRequest state not eligible to run: {str(self._state)}")
        if self._send_pipe:  # Need to pipe results to main process
            try:
                self.update_state(WorkState.STARTED)
                if self._is_generator:
                    for result in self._runnable.run():
                        self._send_pipe.send(result)
                        if globalvars.current_state.no_go:
                            logger.warning(f"Aborting work as no_go is True. Work request: {self}")
                            self.cancel()
                            break
                else:
                    self._send_pipe.send(self._runnable.run())
                if not self._cancelled:
                    self.update_state(WorkState.COMPLETED)
            except exceptions.CancelledError:
                logger.warning("Work was cancelled from inside the executed function.")
                self.update_state(WorkState.CANCELLED)
            except BrokenPipeError:
                logger.error("Broken pipe. It is possible that the work was cancelled from the main process side.")
                self.update_state(WorkState.CANCELLED)
            except Exception:
                logger.exception(f"Exception while running work request {self}")
                self.update_state(WorkState.ERROR)
        else:  # Running in main process
            print("running in place")
            return self._runnable.run()

    def update_state(self, state: WorkState):
        if isinstance(state, WorkState):
            if state is self._state:
                raise exceptions.WorkError(f"Redundant state update: {str(state)}")
            elif state is WorkState.CANCELLED:
                logger.debug(f"Cancelling request: {self}")
            elif self._state in type(self).immutable_states:
                print(str(self._state))
                print(str(state))
                raise exceptions.WorkError(f"It is not allowed to change state from {str(self._state)} to {str(state)}")
            self._state = state
            if self._send_pipe and (not self._send_pipe.closed):
                self._send_pipe.send(state)
        else:
            raise TypeError(f"New state does not appear to be valid: {str(state)}")

    def __repr__(self):
        return "%s(%r, %r, %r, ...) <ID: %r>" % (
            self.__class__.__name__, self._is_generator, self._send_pipe, self._runnable.func, self._id
        )
