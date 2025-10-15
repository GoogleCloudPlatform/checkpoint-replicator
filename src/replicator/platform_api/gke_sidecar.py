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

from file_naming import Volume


from .gke_managed import get_my_ip
from .vm_test import set_config, register_coordinator, get_coordinator, unregister_coordinator, \
     _set_replication_peer_helper, _unmount_peer_helper


def set_replication_peer(local_mountpoint, target_ip):
    peer_dir = f"{Volume.Repl.value}/{local_mountpoint}"
    cmd = f"mount -t nfs -o nconnect=16 {target_ip}:/exports {peer_dir}"
    _set_replication_peer_helper(peer_dir, target_ip, cmd)


def unmount_peer(local_mountpoint):
    peer_dir = f"{Volume.Repl.value}/{local_mountpoint}"
    cmd = f"umount {peer_dir}"
    _unmount_peer_helper(peer_dir, cmd)


from .vm_test import unmount_all_peers, mount_gcs_bucket
