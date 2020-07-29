#!/usr/bin/env python3

"""
This example shows how to use generator functions with worker processes.

In this case, we pass the is_generator keyword argument to processmanager.submit() with a value of True
That will return a list of all items returned from the generator.

The benefit here is that the results are piped back to the main process over time instead of all at once, which can help
smooth out load. (especially when dealing with many results from multiple processes)

Note that this can be slower than a standard function, but the next example will show how to speed things up.

Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'

> python 2_generator_example.py searching large_file.txt
15036 (parent process)
19732 (child process)
1916 (child process)
22212 (child process)
18228 (child process)
560 (child process)
22400 (child process)
9712 (child process)
22100 (child process)
will search for 'searching' in large_file.txt
large_file.txt finished in 6.121107816696167 seconds, result count: 502
total result count: 502, file count: 1, total time: 6.122106313705444 seconds

> python 2_generator_example.py searching larger_file.txt
14676 (parent process)
20968 (child process)
21252 (child process)
4948 (child process)
9696 (child process)
15392 (child process)
18264 (child process)
17240 (child process)
22972 (child process)
will search for 'searching' in larger_file.txt
larger_file.txt finished in 15.845844268798828 seconds, result count: 125822
total result count: 125822, file count: 1, total time: 15.847832918167114 seconds

> python 2_generator_example.py searching large_file.txt larger_file.txt
3068 (parent process)
12564 (child process)
1060 (child process)
14536 (child process)
14836 (child process)
20944 (child process)
8624 (child process)
13168 (child process)
19500 (child process)
will search for 'searching' in large_file.txt
will search for 'searching' in larger_file.txt
large_file.txt finished in 6.2952916622161865 seconds, result count: 502
larger_file.txt finished in 15.907690048217773 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 15.908687114715576 seconds
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


def lines_containing_string_in_file_generator(
        search_string: str,
        filepath: str,
        encoding: str = None,
        errors: str = None
) -> str:
    """Yield lines in a file that contain a given string.

    :param search_string: str item to search for
    :param filepath: str or Path-like object file to search in
    :param encoding: optional str encoding to use in open() call
    :param errors: optional str error handling to pass to open() call
    :return: str containing search string found in file
    """
    with open(filepath, mode="r", encoding=encoding, errors=errors) as file_handle:
        for line in file_handle:
            if search_string in line:
                yield line


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

        # Note that this time we pass is_generator during submission
        future_references[filename] = processmanager.submit(
            lines_containing_string_in_file_generator,
            args=(provided_search_string, filename),
            kwargs={"errors": "replace"},
            is_generator=True
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
