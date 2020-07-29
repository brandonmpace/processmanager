#!/usr/bin/env python3

"""
This example shows how to use generator functions with worker processes with customized result handling.

In this case, we pass the is_generator keyword argument to processmanager.submit() with a value of True
We also create a subclass of ResultHandler that will encapsulate the chunking for us.
The subclass is passed to processmanager.submit() via the handler_class keyword argument.

The benefit here is that the results are not piped back as they are generated, which allows finding a balance with the
per-result IPC (inter-process communication) overhead and load in the main process.
Chunking can be faster than streaming the results back when there are many results.

Below are results from my system looking for the word 'searching' in two files:
 - a 1.5GB file 'large_file.txt' and a 3GB file 'larger_file.txt'

Note that with enough files and results to saturate more of the worker processes this chunking would be more beneficial.
When I made copies of the two test files and provided 4 files to the script, this was 1 second faster than the previous
example. (not shown for brevity, but it was ~18.04s compared to previous example being ~19.05s)

> python 3_chunked_generator_example.py searching large_file.txt
13924 (parent process)
14180 (child process)
11424 (child process)
2952 (child process)
15960 (child process)
21636 (child process)
10488 (child process)
10864 (child process)
22796 (child process)
will search for 'searching' in large_file.txt
large_file.txt finished in 6.990582704544067 seconds, result count: 502
total result count: 502, file count: 1, total time: 6.991548538208008 seconds

> python 3_chunked_generator_example.py searching larger_file.txt
17764 (parent process)
15608 (child process)
22376 (child process)
20948 (child process)
21616 (child process)
21536 (child process)
12536 (child process)
16552 (child process)
17884 (child process)
will search for 'searching' in larger_file.txt
larger_file.txt finished in 15.498421430587769 seconds, result count: 125822
total result count: 125822, file count: 1, total time: 15.498421430587769 seconds

> python 3_chunked_generator_example.py searching large_file.txt larger_file.txt
14040 (parent process)
22964 (child process)
20780 (child process)
20964 (child process)
4976 (child process)
2536 (child process)
18648 (child process)
12716 (child process)
284 (child process)
will search for 'searching' in large_file.txt
will search for 'searching' in larger_file.txt
large_file.txt finished in 7.0553059577941895 seconds, result count: 502
larger_file.txt finished in 15.586554050445557 seconds, result count: 125822
total result count: 126324, file count: 2, total time: 15.587551593780518 seconds
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


def lines_containing_string_in_file_generator(
        search_string: str,
        filepath: str,
        encoding: str = None,
        errors: str = None,
        chunk_size: int = 5000
) -> List[str]:
    """Yield lines in a file that contain a given string.

    :param search_string: str item to search for
    :param filepath: str or Path-like object file to search in
    :param encoding: optional str encoding to use in open() call
    :param errors: optional str error handling to pass to open() call
    :param chunk_size: int number of results to send back at a time
    :return: lists of strings that were found
    """
    with open(filepath, mode="r", encoding=encoding, errors=errors) as file_handle:
        search_results = []
        for line in file_handle:
            if search_string in line:
                search_results.append(line)

            if len(search_results) == chunk_size:
                yield search_results
                search_results = []

        if search_results:
            # Make sure to send back the last chunk
            yield search_results


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

        # Note that this time we are passing a handler_class reference
        future_references[filename] = processmanager.submit(
            lines_containing_string_in_file_generator,
            args=(provided_search_string, filename),
            kwargs={"errors": "replace"},
            handler_class=ChunkedResultHandler,
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
