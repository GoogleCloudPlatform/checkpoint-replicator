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
import pprint
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import coordinator
import file_naming
import platform_api
import sync
import util
import yaml
from file_naming import *
from metrics_manager import *


def list_meta_files(config, path: Path):
    res = []
    for file in path.glob("*.meta"):
        if step_node_gpu := parse_checkpoint_filename(file.name, config["job-name"], extension="meta"):
            with open(file, "r") as f:
                hash = f.read()
            res.append(step_node_gpu + (hash,))
        else:
            logging.warning(f"Unexpected meta file name format: {file}")

    return res


def list_data_files(config, path: Path):
    file_pattern = re.compile(r"(D?[a-z0-9]{64})\.data$")

    res = []
    for file in path.glob("*.data"):
        logging.debug(f"Found data file: {file}")
        if m := file_pattern.match(file.name):
            res.append(m[1])
        else:
            logging.warning(f"Unexpected data file name format: {file}")

    return res


def copy_file_if_exists(src, dst):
    if src.exists():
        # on restore path copy at max speed by using many workers
        sync.atomic_copy_file(src, dst, max_workers=util.get_max_workers(), force=True)
        return True
    else:
        logging.info(f"File not found: {src}")
        return False


def fetch_file(file_name, peers, backup_src, always_copy):
    local = Volume.Local.value

    # Fetch file if missing or if forced
    if always_copy or not Path(local, file_name).exists():
        found = False
        # try to restore from peers first
        for peer in peers:
            # using peer IP address as mount dir name
            source = ensure_peer_mounted(peer, peer)
            source = Path(source, file_name)
            if copy_file_if_exists(source, local):
                found = True
                break

        # try backup if possible
        if not found and backup_src is not None:
            source = Path(backup_src, file_name)
            found = copy_file_if_exists(source, local)

        if not found:
            raise IOError(f"Missing file: {file_name}")


def fetch_missing_data_files(config, step, meta_files, data_sources, backup_src, always_create_meta):
    # unmount left-over peers if any
    platform_api.unmount_all_peers()

    job_name = config["job-name"]
    node_rank = config["node-rank"]

    local = Volume.Local.value
    data_hashes = set()
    for gpu_rank, data_hash in meta_files:
        file_base = build_checkpoint_filename(
            job_name, step, node_rank, gpu_rank)
        meta_file = file_base + ".meta"

        # Create .meta file if missing or if forced
        meta_file_path = Path(local, meta_file)
        if always_create_meta or not meta_file_path.exists():
            logging.info(f"Creating .meta file: {meta_file} -> {data_hash}")
            meta_file_tmp_path = Path(local, meta_file + ".tmp")
            with open(meta_file_tmp_path, "w") as f:
                f.write(data_hash)
            os.rename(meta_file_tmp_path, meta_file_path)

        # we do copy in 2 steps because multiple .meta files can point to the same .data file
        data_hashes.add(data_hash)

    with ThreadPoolExecutor(max_workers=len(data_hashes)) as executor:
        futures = []
        for data_hash in data_hashes:
            # Copy .data file if missing
            data_file_peers = data_sources.get(data_hash, [])
            data_file = data_hash + ".data"
            futures.append(executor.submit(fetch_file, data_file, data_file_peers, backup_src, always_copy=False))

        failures = util.wait_futures(futures, f"Fetching data files for checkpoint {step}")
        if not failures:
            logging.info(f"All files for checkpoint {step} are available locally now")


def mount_replication_peers(config, peers):
    logging.info(f"My replication peers: {peers}")
    platform_api.unmount_all_peers()

    for peer_ip, peer_subdir in zip(peers, range(len(peers))):
        peer_subdir = str(peer_subdir)
        platform_api.set_replication_peer(peer_subdir, peer_ip)


_ensure_peer_mounted_lock = Lock()


def ensure_peer_mounted(local_mountpoint, target_ip):
    peer_dir = os.path.join(Volume.Repl.value, local_mountpoint)
    # Double-checked-locking would NOT work here!
    # Because dir creation and mounting are not atomic.
    with _ensure_peer_mounted_lock:
        if not os.path.exists(peer_dir):
            platform_api.set_replication_peer(local_mountpoint, target_ip)
        else:
            logging.info(f"Dir {peer_dir} is already mounted")

    return peer_dir


def create_restore_links(config, step):
    job_name = config["job-name"]
    node_rank = config["node-rank"]
    workers = config["workers-per-node"]

    for gpu_rank in range(workers):
        threading.Thread(target=create_restore_link,
                         args=(step, job_name, node_rank, gpu_rank),
                         daemon=True
                         ).start()


def create_restore_link(step, job_name, node_rank, gpu_rank):
    local = Volume.Local.value

    file_base = build_checkpoint_filename(job_name, step, node_rank, gpu_rank)

    meta_file = file_base + ".meta"

    with open(Path(local, meta_file), "r") as f:
        hash = f.read()

    data_file = hash + ".data"

    restore_file = build_checkpoint_filename(job_name, step, node_rank, gpu_rank, "restore")
    restore_file_path = Path(local, restore_file)

    # symlink creation is atomic, so no need for .tmp file
    os.symlink(data_file, restore_file_path)

    logging.info(f"Created restore link: {restore_file} -> {data_file}")


def create_empty_restore_files(config):
    job_name = config["job-name"]
    node_rank = config["node-rank"]
    workers = config["workers-per-node"]

    for gpu_rank in range(workers):
        threading.Thread(target=create_empty_restore_file,
                         args=(job_name, node_rank, gpu_rank),
                         daemon=True
                         ).start()


def create_empty_restore_file(job_name, node_rank, gpu_rank):
    local = Volume.Local.value

    restore_file = build_checkpoint_filename(job_name, 0, node_rank, gpu_rank, "restore")
    restore_file_path = Path(local, restore_file)

    # empty file creation is atomic, so no need for .tmp file
    open(restore_file_path, "w").close()

    logging.info(f"Created empty restore file: {restore_file}")


def create_prerestore_file(content : dict):
    prerestore_file =file_naming.REPLICATOR_PRERESTORE_FILE
    tmp_file = str(prerestore_file) + ".tmp"

    # write content dictionary as yaml
    with open(tmp_file, "w") as f:
        yaml.dump(content, f)

    os.rename(tmp_file, prerestore_file)
    logging.info(f"Created .prerestore file '{prerestore_file}': {content}")


def run(config, master):
    local_path = Path(Volume.Local.value)
    req = {
        "request": "restore",
        "node-rank": config["node-rank"],
        "node-ip": platform_api.get_my_ip(),
        "meta-files": list_meta_files(config, local_path),
        "data-files": list_data_files(config, local_path),
    }

    peer_ranks = config.get("peer-ranks")
    if peer_ranks is not None:
        req["peer-ranks"] = peer_ranks

    with MetricManager().node_operations_latency({"method_name": "CoordinatorRegistration", "framework": ""}):
        logging.info(f"Sending restore request:\n{pprint.pformat(req, width=120, compact=True)}")
        master.send_json(req)

        resp: dict = master.recv_json()
        logging.info(f"Received restore response:\n{pprint.pformat(resp, width=120, compact=True)}")

    restore_step = resp["restore-version"]
    restore_backup = resp.get("restore-backup")
    replication_peers = resp["replication-peers"]

    # overwrite the number of peers per node as coordinator may provide less than we wanted
    config["peers-per-node"] = len(replication_peers)

    if not isinstance(restore_step, int):
        logging.error(f"Cannot restore, no checkpoint to restore from: {restore_step}")

        # crete empty .prerestore file
        create_prerestore_file({})

        # release job waiting for .restore first
        create_empty_restore_files(config)

        # and then mount peers
        with MetricManager().node_operations_latency({"method_name": "RestorePeerMount", "framework": ""}):
            mount_replication_peers(config, replication_peers)

        # GC everything, not keeping any steps
        sync.garbage_collect_local_storage(config, keep_steps=set())

        return

    prerestore_info = {"restore-step": restore_step}
    if restore_backup is not None:
        prerestore_info["restore-backup"] = restore_backup

    create_prerestore_file(prerestore_info)

    meta_files = resp["meta-files"]
    restoring_from_backup = restore_backup is not None

    if not restoring_from_backup:
        logging.info(f"Restoring from in-cluster checkpoint {restore_step}")
        backup_src = None
    else:
        logging.info(f"Restoring from backup '{restore_backup}', checkpoint {restore_step}")
        backup_src = Path(Volume.Backup.value, restore_backup)

    data_sources = resp.get("restore-peers", {})

    with MetricManager().node_operations_latency({"method_name": "CheckpointRestore", "framework": ""}):
        fetch_missing_data_files(
            config, restore_step, meta_files, data_sources, backup_src, always_create_meta=restoring_from_backup)

    # Reply from Coordinator to restore-done request serves as a barrier before it's OK to run first GC
    req = {
        "request": "restore-done",
        "node-rank": config["node-rank"],
    }
    # Optimized restore is not supported for multiple workers per node
    if config["workers-per-node"] == 1:
        assert len(meta_files) == 1, "Expected just one meta file"
        req["data-hash"] = meta_files[0][1]  # data hash is the second element in the tuple

    logging.info(f"Sending 'restore-done' request:\n{pprint.pformat(req, width=120, compact=True)}")
    master.send_json(req)

    master.recv_json()  # wait for an empty response
    logging.info("Received 'restore-done' response")

    # all restore steps must finish before this point, as creation of the .restore files wakes up training job
    # release job waiting for .restore first
    create_restore_links(config, restore_step)

    # and then mount peers
    with MetricManager().node_operations_latency({"method_name": "PeerMount", "framework": ""}):
        mount_replication_peers(config, replication_peers)

    if "assume-data-parallelism" in config:
        keep_node_rank = config["node-rank"]
        logging.info(f"Only keeping .meta files for my Node {keep_node_rank}")
    else:
        keep_node_rank = None

    sync.garbage_collect_local_storage(config, {restore_step}, keep_node_rank=keep_node_rank)


if __name__ == "__main__":
    from common_main import *
    run(*common_main(coordinator.State.RESTORE_ONLY))
    common_cleanup()
