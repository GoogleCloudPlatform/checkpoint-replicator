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
import multiprocessing
import os
import threading
from pathlib import Path

import coordinator
import platform_api
import util
from file_naming import Volume
from file_watching import wait_for_file
from metrics_manager import *
from prometheus_client import CollectorRegistry, multiprocess, start_http_server, ProcessCollector


def cleanup_dir(dir_name):
    logging.info(f"Cleaning up directory '{dir_name}'")

    # delete everything except the files/dirs we want to keep
    keep = ["*.meta", "*.data", "jax-init-info.txt", "replicator.yaml"]

    for f in Path(dir_name).iterdir():
        if not any(f.match(k) for k in keep):
            util.delete_file_or_dir(f)
        else:
            logging.info(f"Keeping file/dir {f}")


def cleanup_storage():
    cleanup_dir(Volume.Local.value)


def _termination_handler(config_file):
    wait_for_file(config_file)
    logging.error(f"New config `{config_file}` detected. Terminating.")
    # terminate quickly, as container will restart anyway
    os._exit(0)


def terminate_on_new_config(config_file):
    threading.Thread(target=_termination_handler, args=(config_file,), daemon=True).start()


def setup_prometheus():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    ProcessCollector(namespace='', registry=registry)
    start_http_server(port=int(os.getenv("PROMETHEUS_PORT", 8000)), registry=registry)


def test_gcs_bucket_is_hns_enabled(config):
    # it's sufficient for just one node to do the check,
    # as we are testing a global property of the GCS bucket itself
    if config["node-rank"] == 0:
        try:
            logging.info("Testing atomic dir renames on backup volume (GCS bucket is HNS-enabled)")

            backup_dir = Volume.Backup.value
            test_dir = Path(backup_dir, "atomic-rename-test")
            test_dir_renamed = Path(backup_dir, "atomic-rename-test-passed")

            util.delete_file_or_dir(test_dir)
            util.delete_file_or_dir(test_dir_renamed)

            Path(test_dir).mkdir()
            # in order for dir rename to fail on non-HNS bucket the dir must
            # have at least 1 file in it
            test_file = Path(test_dir, "dummy-file")
            test_file.touch()

            test_dir.rename(test_dir_renamed)

            logging.info(f"Confirmed that Backup volume supports atomic renames (GCS bucket is HNS-enabled)")
            util.delete_file_or_dir(test_dir_renamed)

        except Exception as e:
            logging.critical(f"FAILED: Backup volume does NOT support atomic renames (GCS bucket is NOT HNS-enabled): {e}")
            # keeping (not cleaning up) the failed GCS dir on-purpose, as another indicator of the failure


def common_main(initial_state: coordinator.State = coordinator.State.RESTORE):
    multiprocessing.set_start_method("forkserver")
    util.init_logging()

    setup_prometheus()

    config_file = f"{Volume.Local.value}/replicator.yaml"

    wait_for_file(config_file)

    if os.path.getsize(config_file) == 0:
        logging.error(f"Config file '{config_file}' is empty, deleting it and terminating.")
        util.delete_file_or_dir(config_file)
        # terminate quickly, as container will restart anyway
        os._exit(1)

    config = util.read_config(config_file)
    cleanup_storage()

    platform_api.set_config(config)
    # do as little as possible before this call, as it forks Coordinator process
    master = coordinator.init(config, initial_state)

    # wait to delete the config file until after we've connected to the
    # coordinator. If the coordinator is stale, the init will fail and cause
    # this container to restart. We'll try again only if there's a config file
    # to read.
    util.delete_config()

    with MetricManager().node_operations_latency({"method_name": "MountGcsBucket", "framework": ""}):
        platform_api.mount_gcs_bucket(config["job-name"])
    threading.Thread(target=test_gcs_bucket_is_hns_enabled, args=(config,), daemon=True).start()

    terminate_on_new_config(config_file)

    return config, master


def common_cleanup():
    coordinator.stop()
