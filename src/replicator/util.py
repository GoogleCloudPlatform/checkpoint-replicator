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

import concurrent.futures
import logging
import logging.handlers
import os
import shutil
import sys

import file_naming
import yaml
from blake3 import blake3
from file_naming import Volume
from time_block import TimeBlock


trace_level_num = logging.INFO


def init_logging():
    global trace_level_num

    trace_level_str = os.getenv("LOG_LEVEL", "INFO")
    # surprisingly getLevelName works in both directions
    trace_level_num = logging.getLevelName(trace_level_str)
    if not isinstance(trace_level_num, int):
        print(f"Invalid LOG_LEVEL '{trace_level_str}', using default: INFO", flush=True)
        trace_level_num = logging.INFO

    print(f"Logging level set to: {logging.getLevelName(trace_level_num)} ({trace_level_num})", flush=True)

    set_extra_logging_info("init")


def set_extra_logging_info(extra_info: str = ""):
    # need to reset first for basicConfig to have any effect
    logging.getLogger().handlers.clear()

    consoleHandler = logging.StreamHandler(sys.stdout)
    # DEBUG-level traces from watchdog package are too noisy, so filter them out
    consoleHandler.addFilter(lambda record: not record.name.startswith("watchdog."))

    # delay=True means file will only be created on first message written, if any
    errorHandler = logging.handlers.WatchedFileHandler(file_naming.REPLICATOR_ERRORS_FILE, delay=True)
    errorHandler.setLevel(logging.ERROR)

    criticalHandler = logging.handlers.WatchedFileHandler(file_naming.REPLICATOR_FAILED_FILE, delay=True)
    criticalHandler.setLevel(logging.CRITICAL)

    logging.basicConfig(level=trace_level_num,
                        format=f"%(asctime)s.%(msecs)03d %(process)04X:%(thread)04X {extra_info} [%(levelname)-3.3s] %(module)s.%(funcName)s(%(lineno)d): %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[consoleHandler, errorHandler, criticalHandler])


def try_parse_int(s: str):
    try:
        return int(s)
    except ValueError:
        return s


def expand_env_variables(config):
    """
    Recursively expand environment variables in the configuration.
    """
    if isinstance(config, dict):
        return {k: expand_env_variables(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [expand_env_variables(v) for v in config]
    elif isinstance(config, str):
        return try_parse_int(os.path.expandvars(config))
    return config


_config_file = None
config : dict = None # type: ignore


def read_config(file_name):
    global _config_file, config
    _config_file = file_name

    with open(_config_file, "r") as f:
        config = yaml.safe_load(f)

    logging.info(f"Config raw: {config}")

    config = expand_env_variables(config) # type: ignore

    # set some defaults and derived params
    config.setdefault("workers-per-node", 1)
    config.setdefault("master-port", 4242)
    if "peers-per-node" not in config:
        config["peers-per-node"] = len(config["peer-ranks"])

    set_extra_logging_info(f"job={config['job-name']} node={config['node-rank']}/{config['nodes']}")
    logging.info(f"Config expanded: {config}")

    return config


def delete_config():
    os.remove(_config_file)
    logging.info(f"Deleted config file: {_config_file}")


def file_digest(path):
    """Compute a digest of a file."""

    with TimeBlock(f"Hashing file {path}") as tb:
        # Throttling hashing speed on-purpose, as hashing too fast slows down training job.
        # Timings for hashing 123.3 GB dir with 8 files on A3 VM:
        # Threads   Time (sec)    Throughput (GB/s)
        # auto       1.54         80.1
        #   1       24.8           5.0
        #   2       13.4           9.2
        #   3        8.7          14.2
        #   4        6.72         18.3
        #   8        3.49         35.3
        #  10        3.0          41.1
        # 100        1.58         78.0
        # 200        1.8          68.5
        hash = blake3(max_threads=4).update_mmap(path).hexdigest()
        file_size = os.path.getsize(path)
        tb.append_data("blake3_hash", hash)
        tb.append_data("file_size", file_size, "B", log_rate=True)

    return hash, file_size


def delete_file_or_dir(path):
    if not os.path.lexists(path):
        logging.info(f"Nothing to delete: {path}")
        return

    if os.path.islink(path): kind = "link"
    elif os.path.isdir(path): kind = "dir"
    else: kind = "file"

    logging.info(f"Deleting {kind} {path}")
    if kind == "dir":
        shutil.rmtree(path)
    else:
        os.unlink(path)


def get_max_workers():
    cpus = os.cpu_count() or 1
    # use 1/3 of physical cores (which is 1/3 of half of logical cores = 1/6)
    # obtained empirically after perf runs
    workers = max(4, cpus // 6)
    logging.info(f"Using {workers} workers (with {cpus} CPUs available)")
    return workers


def wait_futures(futures, log_suffix):
    res = concurrent.futures.wait(futures)
    failures = [f for f in res.done if not f.done() or f.exception()]
    failure_count = len(failures) + len(res.not_done)
    if failure_count == 0:
        logging.info(
            f"Successful: {log_suffix}")
        return []
    else:
        logging.error(f"Failed: {log_suffix}, {failure_count} failures:")
        for f in failures:
            logging.error(f"Failed: {f.exception()}")
        for f in res.not_done:
            logging.error(f"Cancelled: {f}")
            failures.append(f)

        return failures


def wait_futures_and_raise(futures, log_suffix, ex_type=IOError):
    failures = wait_futures(futures, log_suffix)
    if failures:
        raise ex_type(failures)


if __name__ == "__main__":
    local = Volume.Local.value
    errors_file = file_naming.REPLICATOR_ERRORS_FILE
    failed_file = file_naming.REPLICATOR_FAILED_FILE

    if os.path.exists(failed_file):
        os.remove(failed_file)
    if os.path.exists(errors_file):
        os.remove(errors_file)

    init_logging()
    assert not os.path.exists(errors_file)
    assert not os.path.exists(failed_file)

    logging.debug("my debug")
    assert not os.path.exists(errors_file)
    assert not os.path.exists(failed_file)

    logging.info("my info")
    assert not os.path.exists(errors_file)
    assert not os.path.exists(failed_file)

    logging.warning("my warning")
    assert not os.path.exists(errors_file)
    assert not os.path.exists(failed_file)

    logging.error("my error")
    assert os.path.exists(errors_file)
    assert not os.path.exists(failed_file)

    logging.critical("my critical")
    assert os.path.exists(errors_file)
    assert os.path.exists(failed_file)
