processmanager (public API)
***************************
.. automodule:: processmanager

This document is intended for those using the package in their own projects.

.. note:: Unless otherwise mentioned, items in this document should only be called from the main process.

Main Functions
==============
Starting and Stopping Worker Processes
--------------------------------------
.. note:: The maximum number of worker processes is the available CPU count minus 1 (automatically capped)

.. autofunction:: processmanager.add_init_func

|

.. autofunction:: processmanager.prepare_globals

|

.. autofunction:: processmanager.start_workers

|

.. autofunction:: processmanager.stop

Requesting Work From Worker Processes
-------------------------------------
.. autofunction:: processmanager.submit

Controlling Offload of Work
---------------------------
.. autofunction:: processmanager.disable_fail_open

|

.. autofunction:: processmanager.disable_offload

|

.. autofunction:: processmanager.enable_offload

Custom Handling of Results in the Main Process
----------------------------------------------

It is possible to customize the encapsulated handling of results that occurs in the main process.
This can change the data returned from functions submitted to :func:`processmanager.submit` in useful ways.

For example, raw results can be sent to the main process and you can modify them after arrival.
The modified results would then be returned via the Future object.

.. autoclass:: processmanager.ResultHandler
   :members: cancel, finalize_result, handle_result


Attributes
==========
.. autoattribute:: processmanager.globalvars.cross_process_lock
   :annotation:

|

.. autoattribute:: processmanager.globalvars.current_state
   :annotation:


Exceptions
==========
.. autoexception:: processmanager.WorkError

|

.. autoexception:: processmanager.CancelledError


Helper Functions
================
.. autofunction:: processmanager.current_process_count

|

.. autofunction:: processmanager.processes_started

|

.. autofunction:: processmanager.wait_for_complete_load

|

.. autofunction:: processmanager.wait_for_process_start


Notifications
=============

Notifications are messages sent from the main process to the worker processes.

It is possible to add custom notifications and associate them with custom actions.
These notifications are handled in dedicated threads in all processes, so they are acted on quickly.

.. autofunction:: processmanager.add_custom_notification

|

.. autofunction:: processmanager.enqueue_notification

|

.. autofunction:: processmanager.update_log_level

|

.. autofunction:: processmanager.update_state_value

