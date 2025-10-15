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

from enum import Enum
import os
import re
import typing


class Volume(Enum):
    Local = os.getenv("LOCAL_VOLUME", "local")
    Repl = os.getenv("REPL_VOLUME", "repl")
    Backup = os.getenv("BACKUP_VOLUME", "backup")


REPLICATOR_PRERESTORE_FILE = f"{Volume.Local.value}/replicator.prerestore"
REPLICATOR_ERRORS_FILE = f"{Volume.Local.value}/replicator.errors"
REPLICATOR_FAILED_FILE = f"{Volume.Local.value}/replicator.failed"


def repl_dirs(config):
    return [f"{Volume.Repl.value}/{peer}" for peer in range(config["peers-per-node"])]


# sample file name is: jobName-s5-n0-w2.save


def build_checkpoint_filename(job_name, step, node_rank, gpu_rank, extension=None, volume: typing.Union[Volume, str] = None):
    base_name = f"{job_name}-s{step}-n{node_rank}-w{gpu_rank}"
    if extension:
        base_name += f".{extension}"
    if volume:
        if isinstance(volume, Volume):
            volume = volume.value
        base_name = f"{volume}/{base_name}"
    return base_name


def get_checkpoint_filename_regex(job_name, step=None, node_rank=None, gpu_rank=None, extension=None, volume: Volume = None):
    regex = re.escape(f"{job_name}-")

    if step is not None:
        regex += f"s{step}"
    else:
        regex += "s([0-9]+)"

    regex += re.escape("-")
    if node_rank is not None:
        regex += f"n{node_rank}"
    else:
        regex += "n([0-9]+)"

    regex += re.escape("-")
    if gpu_rank is not None:
        regex += f"w{gpu_rank}"
    else:
        regex += "w([0-9]+)"

    if extension is not None:
        regex += re.escape(f".{extension}") + "$"
    else:
        regex += "\\.[^/]+$"

    if volume is not None:
        regex = re.escape(f"{volume.value}/") + regex

    regex = ".*(?:^|/)" + regex

    return regex


def parse_checkpoint_filename(file_name, job_name, extension=None, volume: Volume = None):
    file_pattern = get_checkpoint_filename_regex(
        job_name, extension=extension, volume=volume)
    if m := re.fullmatch(file_pattern, file_name):
        return (int(m[1]), int(m[2]), int(m[3]))
    else:
        return None


def build_backup_dir(timestamp):
    return timestamp.strftime("%Y-%m-%d_%H-%M")


def get_backup_dir_regex():
    return re.compile(r"(?:^|/)\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$")


def get_backup_dir_glob():
    return r"????-??-??_??-??"
