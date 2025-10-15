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

import util
from file_naming import *
from file_watching import start_regex_file_watcher


def start_file_watcher(config, file_notifications):
    # sample file name is: jobName-s5-n0-w2.save
    file_pattern = get_checkpoint_filename_regex(
        config["job-name"], node_rank=config["node-rank"], extension="save")

    return start_regex_file_watcher(Volume.Local.value, file_pattern, file_notifications, expect_dirs=False)


def wait_for_local_save(config, file_notifications):
    files = set()
    cur_step = None
    while len(files) < config["workers-per-node"]:
        file = file_notifications.get()
        logging.info(f"Registered: {file}")
        (step, node, worker) = parse_checkpoint_filename(file, config["job-name"])
        logging.debug(
            f"Parsed filename into step={step}, node={node}, worker={worker}")
        if cur_step is None:
            cur_step = step
        else:
            assert step == cur_step
            assert node == config["node-rank"]

        files.add(file)

    logging.info(f"All local .save files found for step {cur_step}")
    return files, cur_step


def hash_file(save_file):
    hash, _size = util.file_digest(save_file)
    return hash


def get_replication_workers():
    return util.get_max_workers()
