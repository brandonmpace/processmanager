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

"""Module for handling of notifications from main process to worker processes"""

import enum
import logcontrol
import logging
import os
import queue
import threading

from typing import Callable, Dict, Optional
from . import workerglobals
from . import globalvars
from . import notificationtypes
from . import mainprocessglobals
from . import validation


logger = logging.getLogger(__name__)


# Mapping of enum.IntEnum subclass to Callable handler for custom notifications
notification_map: Dict[enum.EnumMeta, Callable] = {}

# In worker processes only, a reference to the monitoring thread
notification_monitor: Optional['NotificationMonitor'] = None


# Functions for MAIN process

def add_custom_notification(notification_class: enum.EnumMeta, func: Callable):
    """Register a custom notification enum.IntEnum type with associated handler func

    The handler should accept a single argument.

    When you call :func:`enqueue_notification` with a member of the enum,
    the worker processes will pass it to the handler.

    :param notification_class: :class:`enum.IntEnum` subclass
    :param func: Callable that accepts a single argument of an enum member

    .. note:: This must be called before :func:`start_workers` is called. After that, new registrations are not allowed.

    Example::

        # Create your notification type and values
        class UpdateNotification(enum.IntEnum):
            CONFIG = 1  # means configuration file was updated
            JSON = 2  # means json file for data was updated

        # Define a handler for it that will accept a single member of the enum
        def handle_update_notification(message):
            if message is UpdateNotification.CONFIG:
                reload_config()
            elif message is UpdateNotification.JSON:
                reload_json()
            else:
                logger.error(f"Unknown notification message: {message}")

        # Register the handler and notification type
        processmanager.add_custom_notification(UpdateNotification, handle_update_notification)

        # At the relevant time in your program, you can send notifications from the main process:
        processmanager.enqueue_notification(UpdateNotification.CONFIG)

        # All worker processes will get the notification and pass the message to your handler.
    """
    validation.require_main_context()
    with globalvars.local_lock:
        if mainprocessglobals.start_called:
            raise RuntimeError("Notification registration must be performed before starting workers")
        elif notification_class in notification_map:
            raise RuntimeError(
                f"{notification_class} is already registered with {notification_map[notification_class]}"
            )
        validation.validate_custom_notification(notification_class, func)
        notification_map[notification_class] = func


def dispatch_notification(notification):
    """Internal function to send a notification message to all worker processes.

    Except for shutdown or state update messages, this should only be called from the NotificationSender thread.
    """
    validation.require_main_context()
    if globalvars.queues_ready_event.is_set() or isinstance(notification, notificationtypes.TestNotification):
        for notification_queue in mainprocessglobals.notification_queues:
            try:
                notification_queue.put(notification, timeout=5.0)
            except Exception:
                index = mainprocessglobals.notification_queues.index(notification_queue)
                logger.exception(f"Exception while dispatching notification to queue {index}")
    else:
        logger.warning(f"attempt to send notification before processmanager initialized! Type: {type(notification)}")


def enqueue_notification(notification: enum.IntEnum, timeout: Optional[float] = None) -> bool:
    """Add a notification to the queue to be sent to worker processes.

    The NotificationSender thread is responsible for dispatching from the queue to worker processes.

    The NotificationMonitor in each process will call the registered handler with the notification as the argument.

    :param notification: A member of any registered custom notification (an enum member)
    :type notification: enum.IntEnum
    :param timeout: Number of seconds to wait in case the notification queue is full
    :type timeout: float
    :return: True if added to queue, False if the worker processes were not ready yet
    :rtype: bool
    """
    for registered_type in notification_map:
        if notification in registered_type:
            break
    else:  # no break
        raise TypeError(f"{notification} is not a member of any registered notification type")

    return _enqueue_notification(notification, timeout)


def send_initial_test_notification():
    """Internal function to send initial test notification.

    This should only be called from the NotificationSender thread.

    This is done during initialization to confirm that the worker processes can receive notifications.
    """
    validation.require_main_context()
    logger.info("sending initial test notifications")
    dispatch_notification(notificationtypes.TestNotification.INITIAL)
    logger.info("sent initial test notifications")


def send_keepalive_test_notification():
    """Internal function to send a keepalive so that the worker processes know the main process is still functioning.

    This should only be called from the NotificationSender thread.
    """
    validation.require_main_context()
    dispatch_notification(notificationtypes.TestNotification.KEEPALIVE)
    logger.debug("keepalive notification sent")


def send_log_level_notification(log_level: int):
    """Internal function for sending log level notifications."""
    validation.require_main_context()
    for item in notificationtypes.log_level_notifications:
        if log_level == item:
            _enqueue_notification(item)
            break
    else:  # no break
        msg = f"got invalid log level value of {log_level}"
        logger.error(msg)
        raise ValueError(msg)


def send_shutdown_notification(immediate: bool = False):
    """Internal function to send shutdown message to workers"""
    validation.require_main_context()
    if immediate:
        dispatch_notification(notificationtypes.ShutdownNotification.IMMEDIATE)
    else:
        dispatch_notification(notificationtypes.ShutdownNotification.SAFE)
    mainprocessglobals.notification_queue.put(None)


def send_state_notification(value: bool):
    """Internal function to update worker processes about go/no_go state.

    Typically used for keeping in sync with threadmanager and aborting any new/pending requests.
    """
    validation.require_main_context()
    if value == globalvars.current_state.go:
        logger.debug(f"not sending state notification because it matches current state")
        return
    globalvars.current_state.update(value)
    if value:
        dispatch_notification(notificationtypes.StateChangeNotification.GO)
    else:
        dispatch_notification(notificationtypes.StateChangeNotification.NO_GO)
    logger.info(f"updated go state for worker processes to {value} by sending notification")


def start_notification_sender_thread() -> 'NotificationSender':
    """Internal function to start the NotificationSender thread

    :return: NotificationSender thread instance
    """
    validation.require_main_context()
    logger.debug("about to start notification sender thread")
    new_thread = NotificationSender(name=f"NotificationSender", daemon=True)
    new_thread.start()
    logger.debug("started notification sender thread")
    return new_thread


def update_log_level(log_level: int):
    """Update the log level for root logger in worker processes.

    :param log_level: one of the levels from :mod:`logging` module (:const:`logging.DEBUG`, :const:`logging.INFO`, ...)
    :type log_level: int
    :raises: :exc:`ValueError` when input is not equivalent to one of the built-in levels from the :mod:`logging` module
    """
    send_log_level_notification(log_level)


def update_state_value(value: bool):
    """Update the global go/no-go for handling work requests.

    This is commonly used to keep in sync with threadmanager's go/no-go, but can be used without threadmanager as well.

    :param value: When False new/pending work requests will be dropped and work will be aborted/cancelled
    :type value: bool

    .. seealso:: :data:`processmanager.globalvars.current_state` for more information and examples
    """
    send_state_notification(value)


def _enqueue_notification(notification: enum.IntEnum, timeout: Optional[float] = None) -> bool:
    """Internal function to add a notification to the queue for the NotificationSender to send to all worker processes
    """
    validation.require_main_context()
    if globalvars.queues_ready_event.is_set():
        mainprocessglobals.notification_queue.put(notification, timeout=timeout)
        return True
    else:
        # Notifications are dropped until ready to avoid them piling up
        logger.debug(f"Worker processes are not ready, notification dropped: {notification}")
        return False


# Functions for WORKER processes

def handle_notification(message: enum.IntEnum):
    """Internal function for worker process NotificationMonitor to act on a notification"""
    validation.require_worker_context()
    if isinstance(message, notificationtypes.StateChangeNotification):
        new_go_value = bool(message.value)
        globalvars.current_state.update(new_go_value)
        logger.info(f"handled state update, new go value is {new_go_value}")
    elif isinstance(message, notificationtypes.LogLevelNotification):
        # Uses logcontrol as it will make sure the root logger gets the new value (and keep the LoggerGroup synced)
        # It is recommended to use it, but you can also just use custom notifications if you do not want to.
        logcontrol.set_level(message.value)
    elif isinstance(message, notificationtypes.ShutdownNotification):
        workerglobals.stop_running()
        if message == notificationtypes.ShutdownNotification.IMMEDIATE:
            pass  # TODO: make it so that .SAFE will use .join() etc. (if needed)
    elif isinstance(message, notificationtypes.TestNotification):
        if message == notificationtypes.TestNotification.KEEPALIVE:
            logger.debug("got keepalive notification from main process")
    else:
        for custom_type, handler in notification_map.items():
            if isinstance(message, custom_type):
                try:
                    handler(message)
                except Exception:
                    logger.exception(f"Exception in handler for custom notification type {custom_type}")
                break
        else:  # no break
            logger.error(f"unhandled notification message type: {type(message)}")


def start_notification_monitor_thread(worker_id) -> 'NotificationMonitor':
    """Internal function to start the NotificationMonitor thread

    :return: NotificationMonitor thread instance
    """
    validation.require_worker_context()
    global notification_monitor
    logger.debug("starting notification monitor thread")
    notification_monitor = NotificationMonitor(name=f"NotificationMonitor{worker_id}", daemon=True)
    notification_monitor.start()
    return notification_monitor


def validate_initial_test_notification(message) -> bool:
    """Internal function for the worker processes to confirm that the first notification received is the expected item.

    If this test fails, then there is a bug.
    """
    validation.require_worker_context()
    return (
            (isinstance(message, notificationtypes.TestNotification)) and
            (message == notificationtypes.TestNotification.INITIAL)
    )


class NotificationMonitor(threading.Thread):
    """A dedicated thread to monitor the notification queue in the worker process"""
    def run(self):
        try:
            logger.debug("notification monitor starting")

            # Get the first message, which should be a test message
            try:
                initial_message = workerglobals.notification_queue.get(timeout=globalvars.initialization_timeout)
            except queue.Empty:
                logger.error("never received initial test message from the main process, will exit now")
                workerglobals.stop_running()
            else:
                # Takes global lock to make sure the shared dict check is accurate.
                with globalvars.cross_process_lock:
                    if validate_initial_test_notification(initial_message):
                        logger.info("got initial queue test message, setting acknowledgement variable")
                        globalvars.shared_dict["test_success"].append(os.getpid())
                    else:
                        raise ValueError("initial queue test message was incorrect!")

                    if len(globalvars.shared_dict["test_success"]) == globalvars.shared_dict["process_count"]:
                        logger.info("all processes registered successfully, notifying main process")
                        globalvars.queues_ready_event.set()
                    else:
                        logger.debug("other processes still need to register test success, not notifying main process")

                while workerglobals.keep_running:
                    try:
                        new_notification = workerglobals.notification_queue.get(timeout=globalvars.keep_alive_timeout)
                    except queue.Empty:
                        logger.error(
                            f"no notifications from main process in {globalvars.keep_alive_timeout} seconds, will exit"
                        )
                        workerglobals.stop_running()
                        globalvars.work_queue.put(None)
                    else:
                        logger.info(f"got notification type: {type(new_notification)}")
                        handle_notification(new_notification)
                logger.info("notification monitor exiting main loop")
        except Exception:
            logger.exception("Exception in notification monitoring thread")


class NotificationSender(threading.Thread):
    """A dedicated thread to monitor the notification submission queue in the main process"""
    def run(self):
        try:
            logger.debug("notification sender starting")
            with globalvars.cross_process_lock:
                # Send the first message, which will be a test message.
                # Worker processes ACK this by updating the global shared dict
                send_initial_test_notification()

            while True:
                try:
                    new_notification = mainprocessglobals.notification_queue.get(timeout=globalvars.keep_alive_interval)
                except queue.Empty:
                    # If we haven't sent a notification in given interval, send a test keepalive to worker processes.
                    logger.debug(
                        f"hit timeout of {globalvars.keep_alive_interval} seconds, sending keepalive notification"
                    )
                    send_keepalive_test_notification()
                else:
                    if new_notification is None:
                        break
                    logger.info(f"got notification type for sending: {type(new_notification)}")
                    dispatch_notification(new_notification)
            logger.info("notification sender exiting main loop, disabling multi-core offloads")
            mainprocessglobals.disable_offload()
        except Exception:
            logger.exception("Exception in notification sending thread")
