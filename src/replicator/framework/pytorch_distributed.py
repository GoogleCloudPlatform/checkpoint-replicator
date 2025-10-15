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
import os
from pathlib import Path

import merkle
import util
from file_naming import *
from file_watching import start_regex_file_watcher

from .orbax import _wait_for_local_save


# sample dir name is: local/123
dir_pattern = f"{re.escape(Volume.Local.value)}/([0-9]+)$"


def start_file_watcher(config, file_notifications):
    return start_regex_file_watcher(Volume.Local.value, dir_pattern, file_notifications, expect_dirs=True)


def wait_for_local_save(config, file_notifications):
    return _wait_for_local_save(config, dir_pattern, file_notifications)


def hash_file(save_dir):
    logging.warning(f"Hashing {save_dir} as a regular dir")
    hash, _size = merkle.dir_digest(save_dir)
    return hash


def get_replication_workers():
    # throttle replication and backup to make sure we don't slow down training
    # perf runs show that even 2 workers cause a training slowdown
    return 1


if __name__ == "__main__":
    from sys import argv
    import util

    util.init_logging()
    hash_file(Path(argv[1]))
