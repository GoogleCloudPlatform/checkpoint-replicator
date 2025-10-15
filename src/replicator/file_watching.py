# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
import os
from pathlib import Path

from watchdog.events import FileSystemEventHandler, FileMovedEvent, RegexMatchingEventHandler
from watchdog.observers import Observer

import util


class _FileHandler(FileSystemEventHandler):
    def __init__(self, expected_filename, observer):
        self.expected_file = Path(expected_filename)
        self.observer = observer

    def is_expected_file(self, event):
        if event.is_directory:
            return False
        name = event.dest_path if isinstance(
            event, FileMovedEvent) else event.src_path

        # Compare the tail of full path to the expected file
        expected_parts = self.expected_file.parts
        return Path(name).parts[-len(expected_parts):] == expected_parts

    # redundant to on_close
    # def on_created(self, event):
    #     logging.debug(f"on_created: {event}")
    #     if self.is_expected_file(event):
    #         logging.debug(
    #             f"Received an event that file '{self.expected_file}' has been created.")
    #         self.observer.stop()

    def on_closed(self, event):
        logging.debug(f"on_closed: {event}")
        if self.is_expected_file(event):
            logging.info(
                f"Received an event that file '{self.expected_file}' has been created and closed.")
            self.observer.stop()

    def on_moved(self, event):
        logging.debug(f"on_moved: {event}")
        if self.is_expected_file(event):
            logging.info(
                f"Received an event that a file was moved to '{self.expected_file}'.")
            self.observer.stop()


def wait_for_file(expected_filename):
    logging.info(f"Checking if file '{expected_filename}' exists (quick) ...")
    if os.path.exists(expected_filename):
        logging.info(f"File '{expected_filename}' already exists (quick).")
        return

    observer = Observer()
    event_handler = _FileHandler(expected_filename, observer)
    observer.schedule(event_handler, Path(expected_filename).parent, recursive=False)
    observer.start()

    # For simulating race condition during testing:
    # logging.debug("Sleeping after starting observer...")
    # time.sleep(5)
    # logging.debug("Waking up")

    # Check if the file was created after the first check but before we've started the observer
    logging.info(f"Checking if file '{expected_filename}' exists (safe) ...")
    if os.path.exists(expected_filename):
        logging.info(f"File '{expected_filename}' already exists (safe).")
        observer.stop()

    logging.debug(f"Waiting for observer to stop ...")
    observer.join()


def start_regex_file_watcher(path, regex_pattern, notifications_queue, expect_dirs):
    """Starts a file watcher that monitors a directory for new files or directories matching a regex pattern.
    Full paths of created files/dirs are placed into notifications_queue.

    Args:
        path: The directory path to watch.
        regex_pattern: The regex pattern to match against file or directory names.
        notifications_queue: A queue.Queue object where the full path of matching
                             created or moved files/directories will be put.
        expect_dirs: If True, watch for directories. If False, watch for files.

    Returns:
        A watchdog.observers.Observer object that has been started. The caller
        is responsible for calling observer.stop() and observer.join() when done.
    """

    event_type = "dirs" if expect_dirs else "files"
    logging.info(f"Watching for {event_type} matching: {regex_pattern} in {path}")

    event_handler = RegexMatchingEventHandler(regexes=[regex_pattern], ignore_directories=not expect_dirs)

    # For moved events, the handler triggers on source or destination path matching the criteria.
    # We need an additional regex match check because we are only interested in destination matching.
    event_handler.on_moved = lambda event: notifications_queue.put(
        event.dest_path) if event.is_directory == expect_dirs and re.match(regex_pattern, event.dest_path) else None

    created_lambda = lambda event: notifications_queue.put(
        event.src_path) if event.is_directory == expect_dirs else None

    if expect_dirs:
        event_handler.on_created = created_lambda
    else:
        # for files on_closed event is better than on_created because it triggers on fully-written files
        event_handler.on_closed = created_lambda

    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    return observer


if __name__ == "__main__":
    util.init_logging()
    logging.getLogger().setLevel(logging.DEBUG)
    wait_for_file("test.file")
