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

"""Module for types of notifications sent from main process to worker processes to trigger actions"""

import enum
import logging


class LogLevelNotification(enum.IntEnum):
    """Used to communicate a change in log level"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


log_level_notifications = (
    LogLevelNotification.DEBUG,
    LogLevelNotification.INFO,
    LogLevelNotification.WARNING,
    LogLevelNotification.ERROR,
    LogLevelNotification.CRITICAL,
)
assert len(log_level_notifications) == len([level for level in LogLevelNotification])


class ShutdownNotification(enum.IntEnum):
    """Used to indicate a need to exit the process"""
    SAFE = 0
    IMMEDIATE = 1


class StateChangeNotification(enum.IntEnum):
    """Used to propagate threadmanager go/no_go"""
    NO_GO = 0
    GO = 1


class TestNotification(enum.IntEnum):
    """Used to validate that worker processes properly receive items over the notification queue"""
    INITIAL = 0
    KEEPALIVE = 1


# Used when validating custom notifications to make sure they are not one of the reserved types
reserved_notification_types = (LogLevelNotification, ShutdownNotification, StateChangeNotification, TestNotification)
