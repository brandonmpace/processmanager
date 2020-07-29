#!/usr/bin/env python3

"""
This is a comparison to the basic example that uses concurrent.futures.ProcessPoolExecutor instead.

Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'
Note that compared to the basic example where processmanager was used, this took slightly longer on my system.

> python 1_futures_comparison.py searching larger_file2.ctl
30408 (parent process)
will search for 'searching' in large_file.txt
21392 (child process)
21560 (child process)
27360 (child process)
34432 (child process)
22776 (child process)
43268 (child process)
47304 (child process)
28700 (child process)
large_file.txt finished in 6.688280344009399 seconds, result count: 502
total result count: 502, file count: 1, total time: 6.688280344009399 seconds

> python 1_futures_comparison.py searching larger_file.txt
25508 (parent process)
will search for 'searching' in larger_file.txt
23484 (child process)
5004 (child process)
45156 (child process)
41480 (child process)
47052 (child process)
48048 (child process)
34068 (child process)
8216 (child process)
larger_file.txt finished in 14.776671409606934 seconds, result count: 125822
total result count: 125822, file count: 1, total time: 14.777669429779053 seconds

> python 1_futures_comparison.py searching large_file.txt larger_file.txt
38868 (parent process)
will search for 'searching' in large_file.txt
32864 (child process)
20988 (child process)
31428 (child process)
27792 (child process)
27864 (child process)
will search for 'searching' in larger_file.txt
49072 (child process)
5472 (child process)
36548 (child process)
large_file.txt finished in 7.4653918743133545 seconds, result count: 502
larger_file.txt finished in 15.416099548339844 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 15.796086072921753 seconds
"""


import concurrent.futures
import freezehelper
import logcontrol
import multiprocessing
import os
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
    executor = concurrent.futures.ProcessPoolExecutor()

    future_references: Dict[str, concurrent.futures.Future] = {}
    # Need to track runtimes a different way when not using threadmanager
    start_times: Dict[str, float] = {}

    start_time = time.time()

    # Add threads to do the work for each file provided
    for filename in filepaths:
        if filename in future_references:
            print(f"file {filename} was provided twice! (will ignore)")
            continue

        print(f"will search for '{provided_search_string}' in {filename}")
        start_times[filename] = time.time()
        new_future_reference = executor.submit(
            lines_containing_string_in_file, provided_search_string, filename, errors="replace"
        )

        future_references[filename] = new_future_reference

    results: Dict[str, List[str]] = {}

    # Get the results from each submission
    for filename, future_reference in future_references.items():
        try:
            new_result = future_reference.result()
        except Exception as E:
            print(f"Exception occurred during work: {E}")
        else:
            results[filename] = new_result
            file_time = time.time() - start_times[filename]
            print(f"{filename} finished in {file_time} seconds, result count: {len(new_result)}")

    end_time = time.time()

    # Shutting things down (you could add an atexit handler that would address this)
    executor.shutdown()

    total_time = end_time - start_time
    result_count = sum([len(_) for _ in results.values()])

    print(f"total result count: {result_count}, file count: {len(results)}, total time: {total_time} seconds")
