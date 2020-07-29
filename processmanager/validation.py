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

"""Module for validation functions"""


import enum
import freezehelper

from typing import Callable
from . import notificationtypes


def require_worker_context():
    """Internal function used inside functions that should only run in the worker processes"""
    if freezehelper.is_main_process():
        raise RuntimeError("Invalid operation for main process")


def require_main_context():
    """Internal function used inside functions that should only run in the main process"""
    if freezehelper.is_child_process():
        raise RuntimeError("Invalid operation for worker process")


def validate_custom_notification(notification_type: enum.EnumMeta, func: Callable):
    """Internal function used to validate that items passed for a custom notification registration are valid"""
    if not isinstance(notification_type, enum.EnumMeta):
        raise TypeError(f"{notification_type} of type {type(notification_type)} is not derived from enum.IntEnum")

    for reserved_type in notificationtypes.reserved_notification_types:
        if isinstance(notification_type, reserved_type):
            raise TypeError(
                f"{notification_type} type {type(notification_type)} overlaps with reserved type {type(reserved_type)}"
            )

    for member in notification_type.__members__.values():
        if isinstance(member, enum.IntEnum):
            break  # all enum members should be of the same type..
        else:
            raise TypeError("Notification type is not subclass of enum.IntEnum")

    if not callable(func):
        raise TypeError(f"Function {func} does not appear be callable")
