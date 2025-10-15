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
import socket
from time import sleep
import urllib.parse

from file_naming import Volume
import platform_api


def set_config(config):
    global _config
    _config = config


def get_my_ip():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        parsed_master = urllib.parse.urlsplit(_config["master"])
        # there is no actual connection made here as UDP is connectionless,
        # but the local IP address that would be used to talk
        # to this destination is decided
        s.connect((parsed_master.hostname, parsed_master.port))
        ip = s.getsockname()[0]
        logging.info(f"Discovered my IP: {ip}")
        return ip


def register_coordinator(job_name, ip):
	pass


def get_coordinator(job_name):
    master = _config["master"]
    master_name = urllib.parse.urlsplit(master).hostname
    logging.info(f"Resolving coordinator hostname '{master_name}'")
    while True:
        try:
            address = socket.gethostbyname(master_name)
            logging.info(f"Resolved coordinator hostname '{master_name}' to IP: {address}")
            return master
        except socket.gaierror as e:
            logging.info(f"Error resolving coordinator hostname '{master_name}', will retry: {e}")
            sleep(1)


def unregister_coordinator(job_name, ip):
	pass


def _set_replication_peer_helper(peer_dir, target_ip, cmd):
    os.makedirs(peer_dir, exist_ok=True)

    logging.info(f"Mounting {peer_dir} to peer {target_ip}: {cmd}")
    status = os.system(cmd)
    status = os.waitstatus_to_exitcode(status)
    if status > 0:
        logging.critical(f"Failed to mount peer {peer_dir} to {target_ip}")


def set_replication_peer(local_mountpoint, target_ip):
    peer_dir = f"{Volume.Repl.value}/{local_mountpoint}"
    cmd = f"sudo mount -t nfs -o nconnect=16 {target_ip}:{os.getcwd()}/local {peer_dir}"
    _set_replication_peer_helper(peer_dir, target_ip, cmd)


def _unmount_peer_helper(peer_dir, cmd):
    logging.info(f"Unmounting {peer_dir}: {cmd}")
    status = os.system(cmd)
    status = os.waitstatus_to_exitcode(status)
    if status > 0:
        logging.error(f"Failed to unmount peer {peer_dir}")

    os.rmdir(peer_dir)


def unmount_peer(local_mountpoint):
    peer_dir = f"{Volume.Repl.value}/{local_mountpoint}"
    cmd = f"sudo umount {peer_dir}"
    _unmount_peer_helper(peer_dir, cmd)


def unmount_all_peers():
    logging.info("Unmounting all peers")
    for mount_dir in os.listdir(Volume.Repl.value):
        if os.path.isdir(os.path.join(Volume.Repl.value, mount_dir)):
            # platform_api. prefix potentially delegates to another module's unmount_peer, e.g. gke_sidecar.unmount_peer
            platform_api.unmount_peer(mount_dir)


def mount_gcs_bucket(gcs_dir):
    pass
