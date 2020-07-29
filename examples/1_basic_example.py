#!/usr/bin/env python3

"""
This example demonstrates how to:
- start workers
- wait until they are ready
- submit work requests
- retrieve the results

Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'
Note that compared to the base example where all work was in a single process, the per-file speed is about the same as
when each file was handled individually.
When handling multiple files at the same time, this example was much faster.
With a dedicated process handling each file, the total time to get all results was reduced.

> python 1_basic_example.py searching large_file.txt
280 (parent process)
16740 (child process)
2736 (child process)
20968 (child process)
10904 (child process)
4948 (child process)
16164 (child process)
13560 (child process)
22776 (child process)
will search for 'searching' in large_file.txt
large_file.txt finished in 6.052920341491699 seconds, result count: 502
total result count: 502, file count: 1, total time: 6.0538880825042725 seconds

> python 1_basic_example.py searching larger_file.txt
20088 (parent process)
20420 (child process)
5068 (child process)
15996 (child process)
11348 (child process)
13636 (child process)
19392 (child process)
22232 (child process)
8360 (child process)
will search for 'searching' in larger_file.txt
larger_file.txt finished in 14.135682582855225 seconds, result count: 125822
total result count: 125822, file count: 1, total time: 14.136679887771606 seconds

> python 1_basic_example.py searching large_file.txt larger_file.txt
21904 (parent process)
23444 (child process)
15960 (child process)
15324 (child process)
22836 (child process)
22424 (child process)
18256 (child process)
11344 (child process)
14968 (child process)
will search for 'searching' in large_file.txt
will search for 'searching' in larger_file.txt
large_file.txt finished in 6.1364641189575195 seconds, result count: 502
larger_file.txt finished in 14.147745847702026 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 14.148742914199829 seconds
"""

import concurrent.futures
import freezehelper
import logcontrol
import multiprocessing
import os
import processmanager
import sys
import time

from typing import Dict, List


LOG_DIR = os.path.join(freezehelper.executable_dir, "logs")
MAIN_LOG_PATH = os.path.join(LOG_DIR, "example_log.txt")
WORKER_LOG_DIR = os.path.join(LOG_DIR, "workers")


if freezehelper.is_child_process():
    # Worker processes can get each get a unique log file when set in this way (outside of the __main__ check)
    logcontrol.set_log_file(os.path.join(WORKER_LOG_DIR, f"{os.getpid()}_log.txt"), roll_count=0)
    print(f"{os.getpid()} (child process)")
else:
    logcontrol.set_log_file(MAIN_LOG_PATH)
    print(f"{os.getpid()} (parent process)")


def lines_containing_string_in_file(
        search_string: str,
        filepath: str,
        encoding: str = None,
        errors: str = None
) -> List[str]:
    """Return lines in a file that contain a given string.

    :param search_string: str item to search for
    :param filepath: str or Path-like object file to search in
    :param encoding: optional str encoding to use in open() call
    :param errors: optional str error handling to pass to open() call
    :return: list of strings that were found
    """
    matching_lines = []

    with open(filepath, mode="r", encoding=encoding, errors=errors) as file_handle:
        for line in file_handle:
            if search_string in line:
                matching_lines.append(line)

    return matching_lines


if __name__ == "__main__":
    multiprocessing.freeze_support()

    # It is recommended to catch major issues before starting worker processes, for example:
    if len(sys.argv) < 3:
        print(f"Usage: {freezehelper.executable_path} <search string> <filepath> [<filepath> ...]")
        sys.exit(1)

    provided_search_string = sys.argv[1]
    filepaths = sys.argv[2:]

    # Now to start the worker processes
    processmanager.start_workers()

    # and wait for them to be available
    processmanager.wait_for_complete_load(5)

    future_references: Dict[str, concurrent.futures.Future] = {}
    submission_times: Dict[str, float] = {}

    start_time = time.time()

    # Submitting work to be performed in a worker process for each file provided
    for filename in filepaths:
        if filename in future_references:
            print(f"file {filename} was provided twice! (will ignore)")
            continue

        print(f"will search for '{provided_search_string}' in {filename}")

        submission_times[filename] = time.time()

        future_references[filename] = processmanager.submit(
            lines_containing_string_in_file,
            args=(provided_search_string, filename),
            kwargs={"errors": "replace"}
        )

    results: Dict[str, List[str]] = {}

    # Get the results from each Future (note that you can also use concurrent.futures.as_completed() instead)
    for filename, future_reference in future_references.items():
        try:
            new_result = future_reference.result()
        except Exception as E:
            print(f"Exception occurred during work: {E}")
        else:
            results[filename] = new_result
            file_time = time.time() - submission_times[filename]
            print(f"{filename} finished in {file_time} seconds, result count: {len(new_result)}")

    end_time = time.time()

    # Shutting things down (you could add an atexit handler that would address this)
    processmanager.stop()

    total_time = end_time - start_time
    result_count = sum([len(_) for _ in results.values()])

    print(f"total result count: {result_count}, file count: {len(results)}, total time: {total_time} seconds")
