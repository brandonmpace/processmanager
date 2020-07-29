#!/usr/bin/env python3

"""
This is the base for other examples.

It demonstrates running work in threads, while the other examples will use processmanager to utilize multiprocessing.

It also serves as a baseline for handling work in threads within a single process.

Note that multiprocessing is normally used for CPU-bound work and not IO-bound work.
The example used here is usually IO-bound, but is for demonstration purposes.
These examples will show that this IO-bound work can still benefit from multiprocessing when used with large files.
If you were performing a CPU-intensive action on each line (or a batch of lines), then multiprocessing may be useful.

It can also be useful to offload work to other processes in cases where you are running a GUI and want it to remain
responsive while processing many files at the same time.

Another use case might be processing multiple (very) large files across different CPU cores on a system with very fast
storage access.


Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'
Note how handling multiple files in the same process (last run) slows things down for the smaller file.

> python 0_baseline.py searching large_file.txt
will search for 'searching' in large_file.txt
large_file.txt finished in 5.950582981109619 seconds, result count: 502
total result count: 502, file count: 1, total time: 5.951570749282837 seconds

> python 0_baseline.py searching larger_file.txt
will search for 'searching' in larger_file.txt
larger_file.txt finished in 14.042875289916992 seconds, result count: 125822
total result count: 125822, file count: 1, total time: 14.043854475021362 seconds

> python 0_baseline.py searching large_file.txt larger_file.txt
will search for 'searching' in large_file.txt
will search for 'searching' in larger_file.txt
large_file.txt finished in 14.195916414260864 seconds, result count: 502
larger_file.txt finished in 21.179545879364014 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 21.184547424316406 seconds
"""


import freezehelper
import logcontrol
import os
import sys
import threadmanager
import time

from typing import Dict, List


LOG_DIR = os.path.join(freezehelper.executable_dir, "logs")
MAIN_LOG_PATH = os.path.join(LOG_DIR, "example_log.txt")


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
    logcontrol.set_log_file(MAIN_LOG_PATH)

    if len(sys.argv) < 3:
        print(f"Usage: {freezehelper.executable_path} <search string> <filepath> [<filepath> ...]")
        sys.exit(1)

    provided_search_string = sys.argv[1]
    filepaths = sys.argv[2:]

    # Using thread pools to manage work
    tm = threadmanager.ThreadManager("example")
    tm.add_pool("files")

    thread_references: Dict[str, threadmanager.classes.TimedThread] = {}

    start_time = time.time()

    # Add threads to do the work for each file provided
    for filename in filepaths:
        if filename in thread_references:
            print(f"file {filename} was provided twice! (will ignore)")
            continue

        print(f"will search for '{provided_search_string}' in {filename}")
        new_thread = tm.add(
            "files",
            lines_containing_string_in_file,
            args=(provided_search_string, filename),
            kwargs={"errors": "replace"},
            get_ref=True
        )

        thread_references[filename] = new_thread

    results: Dict[str, List[str]] = {}

    # Get the results from each thread
    for filename, thread_reference in thread_references.items():
        try:
            new_result = thread_reference.result()
        except Exception as E:
            print(f"Exception occurred during work: {E}")
        else:
            results[filename] = new_result
            print(f"{filename} finished in {thread_reference.total_runtime()} seconds, result count: {len(new_result)}")

    end_time = time.time()

    tm.shutdown()

    total_time = end_time - start_time
    result_count = sum([len(_) for _ in results.values()])

    print(f"total result count: {result_count}, file count: {len(results)}, total time: {total_time} seconds")
