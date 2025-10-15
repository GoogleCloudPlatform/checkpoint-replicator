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
import grpc
import rpc.replication_pb2 as replication_data
import rpc.replication_pb2_grpc as replication_rpc


_server = "localhost:2112"

_stub : replication_rpc.ReplicationServiceStub = None # type: ignore


def _init_stub():
    global _stub

    if _stub is not None:
        return

    channel = grpc.insecure_channel(_server)
    _stub = replication_rpc.ReplicationServiceStub(channel)


def set_config(config):
	# not needed
	pass


def get_my_ip():
	ip = os.getenv("POD_IP")
	logging.info(f"Got my IP from env var POD_IP: {ip}")
	return ip


def register_coordinator(job_name, ip):
	_init_stub()

	logging.info(f"Registering coordinator for job '{job_name}' to address '{ip}'")
	_stub.RegisterCoordinator(replication_data.RegisterCoordinatorRequest(job_name=job_name, ip=ip))


def get_coordinator(job_name):
	_init_stub()

	logging.info(f"Getting coordinator for job '{job_name}'")
	coord = _stub.GetCoordinator(replication_data.GetCoordinatorRequest(job_name=job_name))
	ip = coord.ip
	logging.info(f"Got coordinator address '{ip}'")
	return ip


def unregister_coordinator(job_name, ip):
	_init_stub()

	logging.info(f"Unregistering coordinator for job '{job_name}' with address '{ip}'")
	_stub.UnregisterCoordinator(replication_data.UnregisterCoordinatorRequest(job_name=job_name, ip=ip))


def set_replication_peer(local_mountpoint, target_ip):
	_init_stub()

	logging.info(f"Setting replication peer for mountpoint '{local_mountpoint}' to address '{target_ip}'")
	_stub.SetReplicationPeer(replication_data.SetReplicationPeerRequest(local_mountpoint=local_mountpoint, target_ip=target_ip))


def unmount_peer(local_mountpoint):
	_init_stub()

	logging.info(f"Unmounting peer for mountpoint '{local_mountpoint}'")
	_stub.UnmountPeer(replication_data.UnmountPeerRequest(local_mountpoint=local_mountpoint))


def unmount_all_peers():
	_init_stub()

	logging.info(f"Unmounting all peer mountpoints")
	_stub.UnmountAllPeers(replication_data.UnmountAllPeersRequest())


def mount_gcs_bucket(gcs_dir):
	_init_stub()

	logging.info(f"Mounting GCS bucket for GCS dir '{gcs_dir}'")
	_stub.MountGCSBucket(replication_data.MountGCSBucketRequest(local_mountpoint=gcs_dir))
