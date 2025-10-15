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
from concurrent.futures import ThreadPoolExecutor
import humanize.filesize
import tensorstore as ts
from blake3 import blake3
from pathlib import Path

import merkle
from metrics_manager import QUICK_BUCKETS, MetricManager
import util
from file_naming import *
from file_watching import start_regex_file_watcher
from time_block import TimeBlock


# sample dir name is: local/123
dir_pattern = f"{re.escape(Volume.Local.value)}/([0-9]+)$"


def start_file_watcher(config, file_notifications):
    return start_regex_file_watcher(Volume.Local.value, dir_pattern, file_notifications, expect_dirs=True)


# when making changes, keep in mind that torch_distributed uses this function as well
def _wait_for_local_save(config, pattern, file_notifications):
    dir = file_notifications.get()
    logging.info(f"Registered: {dir}")
    step = int(re.match(pattern, dir).group(1))
    logging.debug(f"Detected checkpoint dir for step={step}")
    save_dir = Path(dir).with_name(build_checkpoint_filename(config["job-name"], step, config["node-rank"], 0, "save"))
    os.rename(dir, save_dir)
    logging.info(f"Renamed {dir} -> {save_dir}")

    return [save_dir], step


def wait_for_local_save(config, file_notifications):
    return _wait_for_local_save(config, dir_pattern, file_notifications)


def _find_checkpoint_subdir(save_dir):
    logging.debug(f"Looking for Orbax checkpoint subdir in {save_dir}")
    # list all dirs under save_dir looking for one containing file manifest.ocdbt
    for subdir in Path(save_dir).iterdir():
        if not subdir.is_dir():
            continue

        logging.debug(f"Checking {subdir}")
        if (subdir / "manifest.ocdbt").is_file():
            logging.info(f"Found Orbax checkpoint subdir at {subdir}")
            return subdir

    return None


def _hash_key(kvs, key):
    v = kvs.read(key).result().value
    v_hash = blake3(v, max_threads=blake3.AUTO).hexdigest()
    return key, len(v), v_hash


def hash_file(save_dir):
    path = _find_checkpoint_subdir(save_dir)

    if path is None:
        # needed for testing
        logging.warning(f"Hashing {save_dir} as a regular dir")
        hash, _size = merkle.dir_digest(save_dir)
        return hash

    with MetricManager().histogram_client("duration_hashing_tensorstore", "time to hash a TensorStore", QUICK_BUCKETS).time(), TimeBlock(f"Hashing TensorStore at {path}") as tb:
        kvs = ts.KvStore.open({"driver": "ocdbt", "base": f"file://{path.as_posix()}"}).result()

        # Get all keys first
        all_keys = kvs.list().result()

        # parallelize reading and hashing
        with ThreadPoolExecutor(max_workers=util.get_max_workers()) as executor:
            # read from TS in the order keys appear naturally, then sort the results by the key name
            all_hashes = sorted(executor.map(lambda k: _hash_key(kvs, k), all_keys))

        store_hash = blake3()
        keys = 0
        hashed_data = 0
        for key, data_len, v_hash in all_hashes:
            keys += 1
            hashed_data += data_len
            logging.debug(f"TensorStore Entry: {key}: {humanize.filesize.naturalsize(data_len)} {v_hash}")
            kv_hash = f"{key}\t{v_hash}\n"
            store_hash.update(kv_hash.encode("utf-8"))

        store_hash = "D" + store_hash.hexdigest()

        tb.append_data("store_hash", store_hash)
        tb.append_data("keys", keys)
        tb.append_data("hashed_data", hashed_data, "B", log_rate=True)

    return store_hash


def get_replication_workers():
    return util.get_max_workers()


if __name__ == "__main__":
    from sys import argv
    import util

    util.init_logging()
    hash_file(Path(argv[1]))
