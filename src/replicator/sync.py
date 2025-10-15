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
import glob
import logging
import pprint
import queue
import shutil
import subprocess
import time
from pathlib import Path
from threading import Thread

import util
from file_naming import *
from metrics_manager import *
from time_block import *
from watchdog.events import *

DEBUG = False


def debug_pause(msg):
    if DEBUG:
        input(msg)


def copy_file(src, dst, file):
    shutil.copyfile(f"{src}/{file}", f"{dst}/{file}")


def copy_dir_parallel_rec(dir, dst, executor, futures):
    for f in os.listdir(dir):
        sub_file_or_dir = f'{dir}/{f}'
        if os.path.isdir(sub_file_or_dir):
            dst_sub_dir = f'{dst}/{f}'
            os.mkdir(dst_sub_dir)
            copy_dir_parallel_rec(sub_file_or_dir, dst_sub_dir, executor, futures)
        else:
            futures.append(executor.submit(copy_file, dir, dst, f))


def copy_dir_parallel(dir, dst, executor):
    futures = []
    copy_dir_parallel_rec(dir, dst, executor, futures)

    util.wait_futures_and_raise(futures, f"Copy of {dir} to {dst}")


def get_dir_size(dir):
    total_size = 0
    for root, _dirnames, filenames in os.walk(dir):
        for f in filenames:
            full_path = os.path.join(root, f)
            total_size += os.path.getsize(full_path)
    return total_size


def atomic_copy_file(file_or_dir, dest, max_workers, force=False):
    base_name = Path(file_or_dir).name
    dst_name = Path(dest, base_name)
    tmp_name = f"{dst_name}.tmp"

    # do a quick but not thread-safe check first
    if not force and Path(dst_name).exists():
        logging.info(
            f"File {dst_name} already exists, skipping copy (quick check)")
        return

    src_is_dir = os.path.isdir(file_or_dir)

    try:
        logging.info(f"Atomically copying {'dir' if src_is_dir else 'file'} {file_or_dir} to {dst_name}")
        # "lock" the temp file, fail if it exists
        debug_pause("Press Enter to open .tmp file ...")
        # .tmp file is our locking mechanism, so even in force mode we don't want to overwrite it.
        # On recovery from failure or a crash we expect all .tmp files to be cleaned up first.

        if src_is_dir:
            os.mkdir(tmp_name)
        else:
            open(tmp_name, "xb").close()

        # above would throw if file/dir already exists

        # this is a thread-safe check, as we are holding the .tmp file "lock" now
        if not force and Path(dst_name).exists():
            logging.info(
                f"File {dst_name} already exists, skipping copy (thread-safe check)")

            # we've created the .tmp file, so it's our responsibility to clean it up
            # if final file already exists
            logging.info(
                f"Removing empty .tmp file/dir that we've just created {tmp_name}")
            if src_is_dir:
                os.rmdir(tmp_name)
            else:
                os.remove(tmp_name)

            return

        debug_pause("Press Enter to start copying to tmp file ...")

        with TimeBlock(f"copy {file_or_dir} to {tmp_name}") as tb:
            if src_is_dir:
                logging.info(f"Using {max_workers} workers to copy dir {file_or_dir}")
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    copy_dir_parallel(file_or_dir, tmp_name, executor)
                size = get_dir_size(file_or_dir)
                tb.append_data("dir_size", size, "B", log_rate=True)

            else:
                shutil.copy(file_or_dir, tmp_name)
                size = os.path.getsize(file_or_dir)
                tb.append_data("file_size", size, "B", log_rate=True)

        debug_pause(
            "Press Enter to rename tmp file to final name ...")
        logging.debug(f"Renaming {tmp_name} -> {dst_name}")
        os.rename(tmp_name, dst_name)

    except FileExistsError as e:
        logging.info(
            f"{'Dir' if src_is_dir else 'File'} {e.filename} already exists, skipping copy (.tmp file exists): {e}")
    except OSError as e:
        # on GCSFuse volume os.mkdir fails with OSError(5) if dir already exists
        if e.errno == 5:
            logging.info(
                f"{'Dir' if src_is_dir else 'File'} {e.filename} already exists (errno 5), skipping copy (.tmp file exists): {e}")
        else:
            logging.error(f"OSError: Failed to copy {file_or_dir} to {dst_name}: {type(e)} {e}")
            raise
    except Exception as e:
        logging.error(f"Failed to copy {file_or_dir} to {dst_name}: {type(e)} {e}")
        raise


def copy_meta_and_data_files(meta_file, dest: str, copy_data, verify_data):
    # cannot do it earlier, as it requires util.config to be set
    import framework

    # get the data file name
    with open(meta_file, "r") as f:
        hash = f.read()

    src_data_file = Path(meta_file).with_name(hash).with_suffix(".data")

    # we copy the metadata first, so that in case of our failure during data copy
    # hopefully some other peer has the same data
    # it means that it's possible to have "dangling pointers", i.e. metadata files that point
    # to non-existing data files, but never to partially written ones
    logging.info(f"Copying .meta {meta_file} referencing .data {src_data_file} to {dest}")
    # this is a single-file copy
    atomic_copy_file(meta_file, dest, max_workers=1, force=True)

    if copy_data:
        logging.info(f"Copying .data {src_data_file} referenced by .meta {meta_file} to {dest}")
        # throttle replication and backup per framework by specifying max_workers to make sure we don't slow down training
        atomic_copy_file(src_data_file, dest, max_workers=framework.get_replication_workers())
    elif verify_data:
        if not src_data_file.exists():
            logging.error(err := f"Missing source .data {src_data_file} referenced by .meta {meta_file}")
            raise FileNotFoundError(err)

        dst_data_file = Path(dest, hash).with_suffix(".data")
        if dst_data_file.exists():
            logging.info(f"As expected .data {src_data_file} referenced by .meta {meta_file} is found at {dst_data_file}")
        else:
            logging.error(err := f"Missing destination .data {dst_data_file} referenced by .meta {meta_file}")
            raise FileNotFoundError(err)


def wait_for_dest_to_be_ready(config, cur_step, peer_dir):
    # TODO: check that files are actually from dest node, i.e. specify node_rank below instead of *
    dest_mask = build_checkpoint_filename(
        config["job-name"], cur_step, "*", "*", "meta", volume=peer_dir)
    prev_cnt = None
    while (cnt := len(glob.glob(dest_mask))) < config["workers-per-node"]:
        if cnt != prev_cnt:
            prev_cnt = cnt
            logging.info(
                f"Found {cnt} out of {config['workers-per-node']} files at dest, will keep waiting")
        time.sleep(0.3)


def wait_for_global_replication(config, master, cur_step, backup_running : bool):
    req = {
        "request": "sync",
        "node-rank": config["node-rank"],
        "replicated-step": cur_step,
        "backup-running": backup_running
    }
    logging.info(f"Sending sync request:\n{pprint.pformat(req, width=120, compact=True)}")
    master.send_json(req)
    resp: dict = master.recv_json()
    logging.info(f"Received sync response:\n{pprint.pformat(resp, width=120, compact=True)}")

    replicated_step = resp["replicated-step"]
    backup_dir = resp.get("backup-dir")
    return replicated_step, backup_dir


def garbage_collect_local_storage(config, keep_steps: set[int], *, keep_node_rank = None):
    logging.info(
        f"Garbage collecting local storage, keeping steps {keep_steps}, node ranks to keep are limited to {keep_node_rank}")

    listing = subprocess.run(
        ["ls", "-lh", Volume.Local.value],
        capture_output=True,
        text=True,
        check=False
    ).stdout
    logging.info(f"Local store before GC:\n{listing}")

    # 1. Delete all .meta files except the ones matching one of `keep_steps` and maybe `keep_node_rank`
    meta_files_mask = f"{Volume.Local.value}/*.meta"

    keep_meta = set()
    for meta_file in glob.glob(meta_files_mask):
        step_node_gpu = parse_checkpoint_filename(
            meta_file, config["job-name"])
        if step_node_gpu is not None:
            step = step_node_gpu[0]
            node = step_node_gpu[1]
        else:
            # delete all .meta files that don't match our naming convention
            step = None
            node = None

        if step is not None and (step in keep_steps) \
           and (keep_node_rank is None or node == keep_node_rank):
            keep_meta.add(meta_file)
            logging.info(f"Keeping meta {meta_file}")
        else:
            os.unlink(meta_file)
            logging.info(f"Deleted {meta_file}")

    # 2. Find all .data files that kept .meta files are pointing to
    keep_hashes = set()
    for meta_file in keep_meta:
        with open(meta_file, "r") as f:
            hash = f.read()
        keep_hashes.add(hash)
        logging.info(f"Keeping {hash} <- {meta_file}")

    # 3. Delete all .data files not marked to be kept in previous steps
    data_files_mask = f"{Volume.Local.value}/*.data"
    for data_file in glob.glob(data_files_mask):
        hash = Path(data_file).stem
        if hash not in keep_hashes:
            util.delete_file_or_dir(data_file)
            logging.info(f"Deleted {data_file}")

    listing = subprocess.run(
        ["ls", "-lh", Volume.Local.value],
        capture_output=True,
        text=True,
        check=False
    ).stdout
    logging.info(f"Local store after GC:\n{listing}")


DEBUG_BACKUP = os.getenv("DEBUG_BACKUP", False)


def copy_files_in_parallel(op, executor, step, source_meta_files, dest_dir, copy_data, verify_data):
    logging.info(
        f"Starting parallel {op} of step {step} to {dest_dir}")

    # Easier to debug non-parallel:
    # [copy_meta_and_data_files(f, dest_dir, copy_data, verify_data) for f in source_meta_files]

    futures = [executor.submit(
        copy_meta_and_data_files, f, dest_dir, copy_data, verify_data) for f in source_meta_files]
    util.wait_futures(futures, f"{op} for step {step} to {dest_dir}")

    # We need backup to be still running when next step checkpoint is saved to validate
    # various GC behaviours, including deleting the just-saved step in assume-data-parallelism case
    if DEBUG_BACKUP and op == "backup" and copy_data:
        logging.info("Sleeping for 10 seconds")
        time.sleep(10)
        logging.info("Exiting backup thread")


def hash_and_wrap_into_meta(save_file_or_dir, hash_func):
    hash = hash_func(save_file_or_dir)

    data_file_or_dir = Path(save_file_or_dir).with_name(hash).with_suffix(".data")
    # there is no possibility of concurrency at this point, so existence check and rename are safe
    if data_file_or_dir.exists():
        logging.error(f"New step {save_file_or_dir} has the same hash {hash} as previous step. This is very unusual and may indicate that training is not making progress. Deleting just-saved {save_file_or_dir} and pointing .meta to already existing .data.")
        util.delete_file_or_dir(save_file_or_dir)
    else:
        logging.info(f"Renaming {save_file_or_dir} -> {data_file_or_dir}")
        os.rename(save_file_or_dir, data_file_or_dir)

    meta_file = Path(save_file_or_dir).with_suffix(".meta")
    meta_file_tmp = f"{meta_file}.tmp"
    logging.info(f"Creating metadata in {meta_file_tmp} -> {hash}")
    with open(meta_file_tmp, "w") as f:
        f.write(hash)

    logging.info(f"Renaming {meta_file_tmp} -> {meta_file}")
    os.rename(meta_file_tmp, meta_file)

    return meta_file


def backup(backup_executor, backup_step, meta_files, backup_dir, backup_data):
    def measured_backup(op, executor, step, source_meta_files, dest_dir, copy_data, verify_data):
        with MetricManager().node_operations_latency({"method_name": "CheckpointBackup", "framework": ""}):
            copy_files_in_parallel(op, executor, step, source_meta_files, dest_dir,  copy_data, verify_data)

    logging.info(f"Backing up {'all' if backup_data else 'only .meta'} files for step {backup_step} to {backup_dir}")

    backup_thread = Thread(
                        target=measured_backup,
                        args=("backup", backup_executor, backup_step, meta_files, Path(Volume.Backup.value, backup_dir), backup_data, False), daemon=True)
    backup_thread.start()

    return backup_thread


def run(config, master):
    # cannot do it earlier, as it requires util.config to be set
    import framework

    file_notifications = queue.Queue()
    observer = framework.start_file_watcher(config, file_notifications)

    repl_executor = concurrent.futures.ProcessPoolExecutor(
        initializer=util.init_logging,
        max_workers=max(10, config["workers-per-node"]))  # at least 10

    backup_executor = concurrent.futures.ProcessPoolExecutor(
        initializer=util.init_logging,
        max_workers=max(10, config["workers-per-node"]))  # at least 10

    try:
        backup_step = None
        backup_thread = None

        assume_data_parallelism = config.get("assume-data-parallelism")
        if assume_data_parallelism:
            # Only one data-parallel partition (the last one) backs up .data files, the rest only backup .meta
            nodes = config["nodes"]
            dp_partition_size = nodes // assume_data_parallelism
            if (nodes % assume_data_parallelism) != 0:
                logging.error(f"Number of Nodes {nodes} is not divisible by DataParallelism {assume_data_parallelism} = {nodes / assume_data_parallelism}")

            # last partition is responsible for backup of .data files
            backup_data = config["node-rank"] >= (nodes - dp_partition_size)
        else:
            backup_data = True

        while True:
            files, cur_step = framework.wait_for_local_save(config, file_notifications)

            logging.info(f"Hashing files for step {cur_step}")
            meta_files = [repl_executor.submit(hash_and_wrap_into_meta, f, framework.hash_file) for f in files]
            del files
            meta_files = [f.result() for f in meta_files]

            logging.info(f"All .save files are hashed for step {cur_step}")

            with MetricManager().node_operations_latency({"method_name": "CheckpointReplicationTotal", "framework": ""}):
                for peer in repl_dirs(config):
                    logging.info(
                        f"Waiting for repl destination '{peer} to be ready")

                    with MetricManager().node_operations_latency({"method_name": "CheckpointReplication", "framework": ""}):
                        # replicate files
                        wait_for_dest_to_be_ready(config, cur_step, peer)
                        # in case of data parallelism we replicate out only tiny .meta files
                        copy_data = config.get("assume-data-parallelism") is None
                        copy_files_in_parallel(
                            "replication", repl_executor, cur_step, meta_files, peer, copy_data=copy_data, verify_data=True)

            # first check if the previous backup has finished
            if backup_thread is not None and not backup_thread.is_alive():
                logging.info(f"Backup thread for step {backup_step} has finished")
                backup_thread = None
                backup_step = None

            if backup_thread is not None:
                logging.info(f"Backup for step {backup_step} is still running")
            else:
                logging.info(f"Backup is not running")

            # notify Coordinator, and get confirmation that checkpoint is globally replicated
            replicated_step, backup_dir = wait_for_global_replication(config, master, cur_step, backup_running = bool(backup_thread))

            logging.info(
                f"All files were replicated globally for step {replicated_step}")

            # start backup in parallel if directed by Coordinator, but only if not already running
            if backup_dir is not None:
                if backup_thread is None:
                    backup_step = replicated_step
                    backup_thread = backup(backup_executor, backup_step, meta_files, backup_dir, backup_data)
                else:
                    logging.error(f"Backup thread for step {backup_step} is still running, skipping backup")

            # Delete old checkpoint(s)
            if assume_data_parallelism and backup_step is not None and backup_data:
                # we are doing .data backup and it's in progress, so we GC the step that was just-saved,
                # i.e. we are NOT keeping replicated_step
                if replicated_step != backup_step:
                    logging.info(f"GCing last replicated step {replicated_step} because backup of step {backup_step} is still running")
                steps_to_keep = {backup_step}
            else:
                steps_to_keep = {replicated_step}
                if backup_step is not None:
                    steps_to_keep.add(backup_step)

            garbage_collect_local_storage(config, steps_to_keep)

    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    from common_main import *
    run(*common_main(coordinator.State.SYNC))
    common_cleanup()
