#!/usr/bin/env python3

"""
This a more advanced example showing how to use generator functions more efficiently with worker processes.
It also implements chunked results, but splits the work across multiple processes.

This can be useful when performing CPU-intensive operations on lines (or chunks of lines) from a file.

Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'

Notes:
 - The task in this example is IO-bound, so does not benefit from sharing the workload like this.
 - If we were performing a CPU-intensive task on each line or chunk of lines, this example may come out ahead of others.

> python 4_split_workloads.py dropped large_file.txt larger_file.txt
42184 (parent process)
43088 (child process)
49008 (child process)
27592 (child process)
44240 (child process)
45592 (child process)
47904 (child process)
38852 (child process)
5332 (child process)
Counting lines in large_file.txt
Counting lines in larger_file.txt
will search for 'dropped' in large_file.txt
submitting request for offset 0
submitting request for offset 7420278
will search for 'dropped' in larger_file.txt
submitting request for offset 0
submitting request for offset 11976590
large_file.txt finished in 8.234277725219727 seconds, result count: 502
larger_file.txt finished in 17.260777950286865 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 17.263769149780273 seconds

As you can see, the total runtime was similar, but it consumed more computing resources to perform the same work.
(seconds of work per file is the cumulative runtime of all threads used for processing a given file)

Again, the demo task was IO-bound and not CPU-bound. CPU-bound tasks may benefit from this approach.
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


# For handling of the chunks, it is possible to create your own ResultHandler subclass
class ChunkedResultHandler(processmanager.ResultHandler):
    """A ResultHandler subclass that automatically combines chunked lists of results"""

    # Override to handle chunked results from generator functions
    def handle_result(self, result):
        if self.result is None:
            self.result = []
        # You could manipulate or process the results before adding them
        # Note that this work is performed in the main process while the worker process continues working
        self.result.extend(result)


def count_lines_in_file(filepath: str, encoding: str = None, errors: str = None) -> int:
    total_lines = 0

    with open(filepath, mode="r", encoding=encoding, errors=errors) as file_handle:
        for line in file_handle:
            total_lines += 1

    return total_lines


def lines_containing_string_in_partial_file_generator(
        search_string: str,
        filepath: str,
        start_offset: int,
        lines_to_read: int,
        encoding: str = None,
        errors: str = None,
        chunk_size: int = 5000
) -> List[str]:
    """Yield lines in a file that contain a given string.

    :param search_string: str item to search for
    :param filepath: str or Path-like object file to search in
    :param start_offset: int 0-based line offset to begin searching at. (0 means beginning of file)
    :param lines_to_read: int number of lines to search in after the offset
    :param encoding: optional str encoding to use in open() call
    :param errors: optional str error handling to pass to open() call
    :param chunk_size: int number of results to send back at a time
    :return: lists of strings that were found
    """
    with open(filepath, mode="r", encoding=encoding, errors=errors) as file_handle:
        line_offset = 0

        # "fast-forward" the file handle to the start point
        while line_offset < start_offset:
            file_handle.readline()
            line_offset += 1

        search_results = []
        lines_read = 0
        for line in file_handle:
            if lines_read == lines_to_read:
                break
            else:
                lines_read += 1

            if search_string in line:
                search_results.append(line)

            if len(search_results) == chunk_size:
                yield search_results
                search_results = []

        if search_results:
            # Make sure to send back the last chunk
            yield search_results


def submit_partial_file_searches(search_string: str, filepath: str, line_count: int) -> List[concurrent.futures.Future]:
    """This function handles submitting the requests for multiple file chunks"""
    future_items = []

    chunk_size_per_process = int(line_count / 2) + 1

    current_offset = 0
    while current_offset < line_count:
        print(f"submitting request for offset {current_offset}")
        new_future = processmanager.submit(
            lines_containing_string_in_partial_file_generator,
            args=(search_string, filepath, current_offset, chunk_size_per_process),
            kwargs={"errors": "replace"},
            handler_class=ChunkedResultHandler,
            is_generator=True
        )

        future_items.append(new_future)
        current_offset += chunk_size_per_process

    return future_items


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

    future_references: Dict[str, List[concurrent.futures.Future]] = {}
    submission_times: Dict[str, float] = {}

    # line counts are collected before the timer in this example so the main work time can be compared
    line_counts: Dict[str, int] = {}
    for filename in filepaths:
        if filename in line_counts:
            print(f"file {filename} was provided twice! (will ignore)")
            continue
        print(f"Counting lines in {filename}")
        line_counts[filename] = count_lines_in_file(filename, errors="replace")

    start_time = time.time()

    # Submitting work to be performed in a worker process for each file provided
    for filename in filepaths:
        if filename in future_references:
            print(f"file {filename} was provided twice! (will ignore)")
            continue

        print(f"will search for '{provided_search_string}' in {filename}")

        submission_times[filename] = time.time()

        future_references[filename] = submit_partial_file_searches(
            provided_search_string, filename, line_counts[filename]
        )

    results: Dict[str, List[str]] = {}

    # Get the results from each Future (note that you can also use concurrent.futures.as_completed() instead)
    for filename, future_list in future_references.items():
        results[filename] = []
        for future_reference in future_list:
            try:
                new_results = future_reference.result()
            except Exception as E:
                print(f"Exception occurred during work: {E}")
            else:
                results[filename].extend(new_results)

        file_time = time.time() - submission_times[filename]
        print(f"{filename} finished in {file_time} seconds, result count: {len(results[filename])}")

    end_time = time.time()

    # Shutting things down (you could add an atexit handler that would address this)
    processmanager.stop()

    total_time = end_time - start_time
    result_count = sum([len(_) for _ in results.values()])

    print(f"total result count: {result_count}, file count: {len(results)}, total time: {total_time} seconds")
