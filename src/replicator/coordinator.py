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

import datetime
import logging
import os
import pprint
import sys
import time
import traceback
from collections import defaultdict, deque
from enum import Enum
from pathlib import Path

import platform_api
import util
import zmq
from file_naming import *
from restore import list_data_files, list_meta_files


class ZMQAsyncServer:
    def __init__(self, address):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind(address)

    def recv_request(self, req_type):
        client_id = self.socket.recv()
        assert (len(self.socket.recv()) == 0)  # skip the empty frame
        req = self.socket.recv_json()
        node_rank = req.get("node-rank")
        logging.debug(f"Received request from {f'Node={node_rank}, ' if node_rank is not None else ''}client_id={client_id.hex()}:\n{pprint.pformat(req, width=120, compact=True)}")

        if not isinstance(req, dict):
            logging.error(f"Received unexpected message: {req}")
            self.respond_to(client_id, {"error": "Expected JSON dictionary request"})
            return client_id, None
        elif (rec_req := req.get("request")) != req_type:
            logging.error(f"Received unexpected request type '{rec_req}' instead of '{req_type}' from Node {node_rank}")
            self.respond_to(client_id, {"error": f"Expected '{req_type}' request, got '{rec_req}'"})
            return client_id, None

        return client_id, req

    def respond_to(self, client_id, response):
        logging.debug(f"Sending reply to {client_id.hex()}:\n{pprint.pformat(response, width=120, compact=True)}")
        self.socket.send(client_id, zmq.SNDMORE)
        self.socket.send(b"", zmq.SNDMORE)
        self.socket.send_json(response)

    def gather_requests(self, req_type, N):
        requests = {}
        logging.info(f"Collecting '{req_type}' requests from {N} Nodes")
        while len(requests) < N:
            # Receive client id and request
            client_id, req = self.recv_request(req_type)
            # ignore invalid requests
            if req:
                requests[client_id] = req

        logging.info(f"Received '{req_type}' requests from {len(requests)} Nodes, sample request: {next(iter(requests.values()))}")
        return requests


def get_backup_list():
    # first round of filtering using glob
    glob = get_backup_dir_glob()
    glob_matches = Path(Volume.Backup.value).glob(glob)

    # keep only dirs and extract just the filename part
    glob_dirs = [dir.name for dir in glob_matches if dir.is_dir()]

    # keep only dirs that match the regex
    backup_dir_regex = get_backup_dir_regex()
    backup_dirs = [
        dir for dir in glob_dirs if backup_dir_regex.match(dir)]

    # sort from newest to oldest
    backup_dirs.sort(reverse=True)

    return backup_dirs


DEBUG_BACKUP = os.getenv("DEBUG_BACKUP", False)

FORCED_BACKUP_RESTORE_FILE_NAME = "restore.version"


# Represents group of Nodes that are restoring the same data hash
class RestoreGroup:
    def __init__(self, hash):
        # data hash that this restore group needs
        self.hash = hash

        # Nodes that have the data they need and are not serving data to others
        self.ready = set()

        # Nodes transferring data right now
        # mapping of PullingNode->ServingNode ranks
        self.busy = dict()

        # Nodes that need data, but were not paired with a ready Node yet
        self.waiting = set()


# Stores all restore data needed by various methods
class RestoreDB:
    def __init__(self, config, requests):
        self.config = config

        # zmq clientID -> request mapping
        self.requests = requests

        # NodeRank -> NodeIP mapping
        self.node_rank_to_ip = {req["node-rank"]: req["node-ip"] for req in self.requests.values()}

        # NodeRank -> zmq clientID mapping
        # Only used if we respond in stages, to reply to individual nodes
        self.node_rank_to_client_id = None

        # This is a 3-dimensional dictionary:
        # StepNumber -> NodeRank -> Worker/GPUNumber -> DataHash string
        self.global_meta = None

        # This is just a flat set of available data file hashes
        self.global_data = None

        # checkpoint step number we are restoring from
        self.restore_cp = None

        # backup name we are restoring from, or None if we are not restoring from backup
        self.restore_backup_name = None

        # data hash -> deque(NodeRank that has this hash) mapping
        self.hash_to_node_ranks = None

    def build_global_state_from_requests(self):
        self.global_meta = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        self.global_data = set()

        # build global view of all available data received from all nodes
        for req in self.requests.values():
            for (step, node_rank, gpu, hash) in req["meta-files"]:
                # TODO: Validation. Duplicates are possible, and expected, but they must all point to the same hash
                self.global_meta[step][node_rank][gpu] = hash

            for hash in req["data-files"]:
                # duplicates are possible, and expected, so we use a set
                self.global_data.add(hash)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Global Meta from requests:\n{pprint.pformat(self.global_meta, width=120, compact=True)}")
            logging.debug(f"Global Data from requests:\n{pprint.pformat(self.global_data, width=120, compact=True)}")

    def load_global_state_from_backup(self, backup_name: str):
        logging.info(f"Considering backup: {backup_name}")

        backup_meta = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        backup_data = set()

        backup_path = Path(Volume.Backup.value, backup_name)
        for (step, node_rank, gpu, hash) in list_meta_files(self.config, backup_path):
            backup_meta[step][node_rank][gpu] = hash

        for hash in list_data_files(self.config, backup_path):
            backup_data.add(hash)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Global Meta from backup '{backup_name}':\n{pprint.pformat(backup_meta, width=120, compact=True)}")
            logging.debug(f"Global Data from backup '{backup_name}':\n{pprint.pformat(backup_data, width=120, compact=True)}")

        self.global_meta = backup_meta
        self.global_data = backup_data

    def validate_checkpoint(self, step: int) -> bool:
        step_meta = self.global_meta[step]
        node_count = self.config["nodes"]
        # the number of workers (GPUs) per Node we expect
        worker_count = self.config["workers-per-node"]
        for node in range(node_count):
            if node in step_meta:
                all_gpus = step_meta[node]
                for gpu in range(worker_count):
                    if gpu in all_gpus:
                        hash = all_gpus[gpu]
                        if hash not in self.global_data:
                            logging.warning(f"Step {step}, Node {node}, GPU {gpu}, Hash {hash} data is missing")
                            return False
                    else:
                        logging.warning(f"Step {step}, Node {node}, GPU {gpu} meta is missing")
                        return False
            else:
                logging.warning(f"Step {step}, Node {node} all meta is missing")
                return False

        return True

    def build_hash_to_node_ranks(self):
        # a mapping of hash -> [NodeRank] for set of nodes that have this data
        hash_to_node_ranks = defaultdict(set)

        for req in self.requests.values():
            node_rank = req["node-rank"]
            for data_hash in req["data-files"]:
                hash_to_node_ranks[data_hash].add(node_rank)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Hash to Node Ranks:\n{pprint.pformat(hash_to_node_ranks, width=120, compact=True)}")

        # convert values from set to deque, so we can rotate them
        self.hash_to_node_ranks = {k: deque(v) for k, v in hash_to_node_ranks.items()}

    def build_node_rank_to_client_id(self):
        self.node_rank_to_client_id = {req["node-rank"]: client_id for client_id, req in self.requests.items()}

    def set_restore_step(self, restore_cp, backup_name=None):
        self.restore_cp = restore_cp
        self.restore_backup_name = backup_name

    def get_node_meta(self, node_rank):
        assert self.restore_cp is not None, f"get_node_meta called for node {node_rank} but restore_cp is not set"
        return self.global_meta[self.restore_cp][node_rank]


class Coordinator(object):
    def start(self, config, initial_state):
        self.config = config
        util.set_extra_logging_info(f"job={config['job-name']} coord")

        port = config["master-port"]
        transport = "tcp"
        listen_adr = f"{transport}://0.0.0.0:{port}"
        logging.info(f"Starting Coordinator on {listen_adr}")
        self.socket = ZMQAsyncServer(listen_adr)
        self.registered_adr = f"{transport}://{platform_api.get_my_ip()}:{port}"
        platform_api.register_coordinator(config["job-name"], self.registered_adr)

        if initial_state == State.RESTORE_ONLY:
            self.restore()
        else:
            if initial_state == State.RESTORE:
                self.restore()
            self.sync()

    # Sample request:
    # 'request': 'restore',
    # 'node-rank': 0,
    # 'node-ip': '10.128.0.3',
    # 'meta-files': [[5, 1, 1, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'],
    #                [5, 0, 0, '7c58f933804e4d88314661832c46768e99546710e5a3677b5733c882fe8ac3b3']],
    # 'data-files': ['dd4fe8cbc6b3a92327588cb1a66a58e32599a905be1af33d25cb017f4f008546',
    #                'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855']
    def restore(self):
        node_count = self.config["nodes"]  # the number of Nodes we expect
        # the number of workers (GPUs) per Node we expect
        worker_count = self.config["workers-per-node"]

        requests = self.socket.gather_requests("restore", node_count)
        # at this point all nodes have discovered the coordinator
        platform_api.unregister_coordinator(self.config["job-name"], self.registered_adr)

        logging.info("Determining best checkpoint to restore from")

        restore_db = RestoreDB(self.config, requests)

        best_cp = None
        backup_name = None

        forced_backup_restore_file = Path(Volume.Backup.value, FORCED_BACKUP_RESTORE_FILE_NAME)
        if forced_backup_restore_file.exists():
            with open(forced_backup_restore_file, "r") as f:
                backup_name = f.read().strip()
            logging.warning(f"Forcing restore from backup version '{backup_name}'")
            # skip restore from in-cluster logic if restore from backup is forced
        else:
            # look for in-cluster checkpoint
            restore_db.build_global_state_from_requests()
            for step in sorted(restore_db.global_meta.keys(), reverse=True):
                if restore_db.validate_checkpoint(step):
                    best_cp = step
                    break

        if best_cp is not None:
            logging.info(f"We will restore in-cluster checkpoint: {best_cp}")
        else:
            if backup_name:
                logging.warning(f"Restoring from forced backup '{backup_name}'")
                backup_list = [backup_name]
            else:
                logging.warning("No valid checkpoints found in the cluster, trying backups")
                backup_list = get_backup_list()

            for backup in backup_list:
                restore_db.load_global_state_from_backup(backup)
                # normally backup should have only one step
                for step in sorted(restore_db.global_meta.keys(), reverse=True):
                    if restore_db.validate_checkpoint(step):
                        best_cp = step
                        backup_name = backup
                        break

                if best_cp:
                    break

            if best_cp is not None:
                logging.info(f"We will restore checkpoint {best_cp} from backup {backup_name}")
            else:
                logging.error("No valid checkpoints found anywhere")

        restore_db.set_restore_step(best_cp, backup_name)

        done_requests = None

        if backup_name is not None and worker_count == 1:
            # worker_count > 1 is not supported yet
            # the extra complexity is that each Node would belong to multiple restore groups

            logging.info(f"Optimized (broadcasting) restore from backup '{backup_name}'")

            # build .data hash -> RestoreGroup mapping
            restore_groups = {}
            restore_db.build_node_rank_to_client_id()

            # Nodes that can seed other RestoreGroups (former replication peers).
            # These are Nodes outside of RestoreGroup who have the data that RestoreGroup needs.
            # Only used for seeding instead of GCS
            # hash -> set of NodeRanks
            seeders = defaultdict(set)

            # build restore_groups, and seeders
            for req in requests.values():
                node_rank = req["node-rank"]
                node_meta = restore_db.get_node_meta(node_rank)
                node_has = set(req["data-files"])
                # we don't do `set(node_meta.values())` here because there could be extra files
                # reported that are not actually needed for this restore
                node_needs = {node_meta[gpu] for gpu in range(worker_count)} # type: ignore
                for need_hash in node_needs:
                    # efficient version of
                    # rg = restore_groups.setdefault(need_hash, RestoreGroup(need_hash))
                    rg = restore_groups.get(need_hash)
                    if rg is None:
                        rg = RestoreGroup(need_hash)
                        restore_groups[need_hash] = rg

                    if need_hash in node_has:
                        # this node has the data it needs
                        rg.ready.add(node_rank)
                    else:
                        # this node needs to pull the data
                        rg.waiting.add(node_rank)

                # Due to replication, some Nodes also have data unrelated to their needs
                for extra_hash in node_has - node_needs:
                    seeders[extra_hash].add(node_rank)

            # Cross-pollination of RestoreGroups
            # Seed the groups with no in-group data from other Nodes in the cluster
            logging.info("Seeding empty restore groups from other Nodes")

            # restore groups that don't contain the data they need on any member Node
            empty_groups = {hash for hash, rg in restore_groups.items() if not rg.ready}

            # how many different RestoreGroups each seeder can seed
            seeder_weights = defaultdict(int)
            for rg_seeders in seeders.values():
                for node in rg_seeders:
                    seeder_weights[node] += 1

            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f"Seeders: {seeders}")
                logging.debug(f"Seeder weights: {seeder_weights}")
                logging.debug(f"Empty groups: {empty_groups}")

            # seed empty groups
            while True:
                # Only consider still empty RestoreGroups with seeders available
                empty_groups = {hash for hash in empty_groups if not rg.busy and len(seeders[hash]) > 0}

                if not empty_groups:
                    # no more in-cluster seeding is possible
                    break

                # find RestoreGroup needing the rarest, most scarcely available file (with least seeders)
                rg_to_seed = min(empty_groups, key=lambda hash: len(seeders[hash]))

                possible_seeders = seeders[rg_to_seed]
                # best seeder is the one with lowest weight (can seed the fewest RestoreGroups)
                serving_node_rank = min(possible_seeders, key=lambda seeder: seeder_weights[seeder])

                # remove this node from seeders for all groups,
                # so it would not seed more than one group
                for rg_seeders in seeders.values():
                    rg_seeders.discard(serving_node_rank)

                rg = restore_groups[rg_to_seed]

                # find a pulling node that is unlikely to be a seeder for other groups itself (non-seeders have the lowest weight=0)
                pulling_node_rank = min(rg.waiting, key=lambda node: seeder_weights[node])
                rg.waiting.remove(pulling_node_rank)
                logging.info(f"RestoreGroup {rg_to_seed}: Node {pulling_node_rank} will pull from Node {serving_node_rank} to seed this group")
                rg.busy[pulling_node_rank] = f"seeder {serving_node_rank}"  # Special case when pulling from another RestoreGroup

                # send the response to the pulling Node
                resp = self._build_restore_response_base(pulling_node_rank, restore_db)
                resp["restore-peers"] = {rg_to_seed: [restore_db.node_rank_to_ip[serving_node_rank]]}
                self.socket.respond_to(restore_db.node_rank_to_client_id[pulling_node_rank], resp)

            # Seed the groups with no in-cluster data from GCS backup
            logging.info(f"Seeding empty restore groups from backup '{backup_name}'")
            for hash, rg in restore_groups.items():
                if not rg.ready and not rg.busy:
                    # find a pulling node that is unlikely to be a seeder for other groups itself (non-seeders have the lowest weight=0)
                    pulling_node_rank = min(rg.waiting, key=lambda node: seeder_weights[node])
                    rg.waiting.remove(pulling_node_rank)
                    logging.info(f"RestoreGroup {hash}: Node {pulling_node_rank} will pull from backup '{backup_name}' to seed this group")
                    rg.busy[pulling_node_rank] = f"backup '{backup_name}'"  # Special case when pulling from backup

                    # send the response to the pulling Node
                    resp = self._build_restore_response_base(pulling_node_rank, restore_db)
                    # no restore-peers set
                    self.socket.respond_to(restore_db.node_rank_to_client_id[pulling_node_rank], resp)

            # Reply to Nodes that have all the data they need
            logging.info(f"Sending 'restore' responses to Nodes that already have the data they need")
            for hash, rg in list(restore_groups.items()):
                for node_rank in rg.ready:
                    # send the response to the done Node
                    resp = self._build_restore_response_base(node_rank, restore_db)
                    # no restore-peers set
                    self.socket.respond_to(restore_db.node_rank_to_client_id[node_rank], resp)

                if not rg.waiting and not rg.busy:
                    # all Nodes in this group are done
                    del restore_groups[hash]
                    logging.info(f"RestoreGroup {hash}: all Nodes already have the data they need")
                else:
                    logging.info(f"RestoreGroup {hash}: {len(rg.waiting)} Nodes are missing the data, {len(rg.busy)} are seeding it, and {len(rg.ready)} Nodes already have it")
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f"RestoreGroup {hash}: initial state: ready={rg.ready}, busy={rg.busy}, waiting={rg.waiting}")

            # Do initial pairing of ready and waiting Nodes
            logging.info(f"Initial pairing of ready and waiting Nodes")
            for rg in restore_groups.values():
                self._pair_up_ready_and_waiting_nodes(rg, restore_db)

            # we'll collect 'restore-done' requests from all nodes here, and then will reply to all of them at the end
            done_requests = {}

            # Proceed in stages, reacting to replies from busy Nodes that has finished pulling data, until all Nodes are done.
            # This is an exponentially-growing broadcast as for every 1 previously serving Node we put 2 Nodes into ready set.
            while len(done_requests) < node_count:
                # Wait for a done reply from any Node that was busy restoring data
                client_id, resp = self.socket.recv_request("restore-done")
                # ignore invalid requests
                if not resp:
                    continue

                done_requests[client_id] = resp

                resp_node_rank = resp["node-rank"]
                hash = resp["data-hash"]

                rg = restore_groups.get(hash)
                if rg is None or resp_node_rank not in rg.busy:
                    logging.debug(f"RestoreGroup {hash}: Ready Node {resp_node_rank} confirmed restore is done")
                    continue

                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f"RestoreGroup {rg.hash}: state before processing 'restore-done' request: ready={rg.ready}, busy={rg.busy}, waiting={rg.waiting}")

                pulling_node_rank = resp_node_rank
                rg.ready.add(pulling_node_rank)
                serving_node_rank = rg.busy.pop(pulling_node_rank)
                if isinstance(serving_node_rank, str):
                    logging.info(f"RestoreGroup {hash}: Node {pulling_node_rank} finished seeding from {serving_node_rank}")
                else:
                    assert isinstance(serving_node_rank, int), f"Expected int serving_node_rank: {serving_node_rank}"
                    rg.ready.add(serving_node_rank)
                    logging.debug(f"RestoreGroup {hash}: Node {pulling_node_rank} finished pulling from Node {serving_node_rank}")

                if not rg.waiting and not rg.busy:
                    # all Nodes in this group are done
                    del restore_groups[hash]
                    logging.info(f"RestoreGroup {hash}: all Nodes are done, {len(restore_groups)} active restore groups left")
                else:
                    self._pair_up_ready_and_waiting_nodes(rg, restore_db)

            assert not restore_groups, "Since all Nodes have reported restore completion, we expect no active restore groups left"
            logging.info(f"All {len(requests)} Nodes have staged checkpoint {best_cp} locally from backup '{backup_name}'")
        else:
            # normal restore

            # need to prepare an additional data structure
            restore_db.build_hash_to_node_ranks()

            # send a reply back to all clients
            logging.info(f"Sending 'restore' responses to {len(requests)} Nodes")
            for client_id, req in requests.items():
                resp = self._build_restore_response_base(req, restore_db)
                self._set_restore_peers(resp, req, restore_db)
                self.socket.respond_to(client_id, resp)

            logging.info(f"Sent 'restore' responses to {len(requests)} Nodes, sample response: {resp}")

            # No point in exchanging 'restore-done' messages if we are not actually restoring anything
            if best_cp is not None:
                done_requests = self.socket.gather_requests("restore-done", node_count)

        # it is None if we are not restoring from anywhere
        if done_requests:
            # This is our global restore completion barrier, preventing premature GC by nodes that finish restore first
            logging.info(f"Responding to 'restore-done' requests from {len(done_requests)} Nodes")

            for client_id in done_requests.keys():
                self.socket.respond_to(client_id, {})

            logging.info(f"Restore complete. Sent 'restore-done' responses to {len(done_requests)} Nodes")
        else:
            logging.info(f"Restore complete, could not find a checkpoint to restore from")


    # Sample request:
    # "request": "sync"
    # "node-rank": 0
    # "replicated-step": 5
    # "backup-running": false
    def sync(self):
        node_count = self.config["nodes"]  # the number of Nodes we expect
        backup_interval = self.config["backup-interval-minutes"] * 60
        last_backup = time.time()

        forced_backup_restore_file = Path(Volume.Backup.value, FORCED_BACKUP_RESTORE_FILE_NAME)

        while True:
            requests = self.socket.gather_requests("sync", node_count)

            # at this point we know all Nodes have restored from forced backup (if it was requested),
            # and saved a fresher checkpoint, so it's OK to delete forced restore file if it exists
            if forced_backup_restore_file:
                if forced_backup_restore_file.exists():
                    logging.warning(f"Deleting forced restore file {forced_backup_restore_file}")
                    forced_backup_restore_file.unlink()

                # only check once
                forced_backup_restore_file = None

            cur_step = None
            for req in requests.values():
                if cur_step is None:
                    cur_step = req["replicated-step"]
                else:
                    assert cur_step == req["replicated-step"]

            resp = {
                "replicated-step": cur_step
            }

            now = time.time()
            if (now - last_backup) >= backup_interval or (DEBUG_BACKUP and cur_step % 2):
                nodes_running_backup = set()
                for req in requests.values():
                    if req["backup-running"]:
                        nodes_running_backup.add(req["node-rank"])

                # request backup if it's empty
                if not nodes_running_backup:
                    last_backup = now

                    utc_now = datetime.datetime.now(datetime.timezone.utc)
                    backup_dir = build_backup_dir(utc_now)

                    logging.info(f"Requesting Nodes to backup step {cur_step} to {backup_dir}")

                    dest = Path(Volume.Backup.value, backup_dir)
                    os.makedirs(dest, exist_ok=True)

                    resp["backup-dir"] = backup_dir
                else:
                    logging.error(f"Backup should be requested for step {cur_step}, but skipping it because some Nodes are still running previous backup: {nodes_running_backup}")

            logging.info(f"Sending 'sync' responses to all Nodes")

            for client_id, _ in requests.items():
                self.socket.respond_to(client_id, resp)

            logging.info(f"Sent 'sync' responses to all Nodes, sample response: {resp}")

    def _pair_up_ready_and_waiting_nodes(self, rg, restore_db):
        # pair up ready and waiting Nodes
        while rg.ready and rg.waiting:
            pulling_node_rank = rg.waiting.pop()
            serving_node_rank = rg.ready.pop()

            logging.debug(f"RestoreGroup {rg.hash}: Node {pulling_node_rank} will pull from Node {serving_node_rank}")

            rg.busy[pulling_node_rank] = serving_node_rank

            # send the response to the pulling Node
            resp = self._build_restore_response_base(pulling_node_rank, restore_db)
            resp["restore-peers"] = {rg.hash: [restore_db.node_rank_to_ip[serving_node_rank]]}
            self.socket.respond_to(restore_db.node_rank_to_client_id[pulling_node_rank], resp)

    def _get_replication_peers_for_node(self, req, restore_db):
        node_rank = req["node-rank"]
        peers = req.get("peer-ranks")
        if peers is None:
            # calculate peers
            peers = self._get_node_peer_ranks(node_rank, restore_db)

        # send back peer IPs
        peer_ips = [restore_db.node_rank_to_ip[n] for n in peers]
        logging.debug(f"Peers for Node {node_rank}: {peers}, their IPs: {peer_ips}")
        return peer_ips

    def _build_restore_response_base(self, req_or_node_rank, restore_db):
        if isinstance(req_or_node_rank, dict):
            req = req_or_node_rank
            node_rank = req["node-rank"]
        else:
            assert isinstance(req_or_node_rank, int), f"req_or_node_rank is expected to be a dict or an int, got {req_or_node_rank}"
            node_rank = req_or_node_rank
            req = restore_db.requests[restore_db.node_rank_to_client_id[node_rank]]

        resp = {"restore-version": restore_db.restore_cp or "No valid checkpoints found"}

        resp["replication-peers"] = self._get_replication_peers_for_node(req, restore_db)

        # will only be set for restore from backup, not for local restore
        if restore_db.restore_backup_name is not None:
            resp["restore-backup"] = restore_db.restore_backup_name

        if restore_db.restore_cp is not None:
            node_meta = restore_db.get_node_meta(node_rank)

            # send the meta data for the checkpoint we are restoring from
            resp["meta-files"] = list(node_meta.items())

        return resp

    def _set_restore_peers(self, resp, req, restore_db):
        if restore_db.restore_cp is None:
            return

        assert restore_db.hash_to_node_ranks is not None

        node_has = set(req["data-files"])

        node_rank = req["node-rank"]
        node_meta = restore_db.get_node_meta(node_rank)
        worker_count = restore_db.config["workers-per-node"]
        # we don't do `set(node_meta.values())` here because there could be extra files
        # reported that are not actually needed for this restore
        node_needs = {node_meta[gpu] for gpu in range(worker_count)} # type: ignore

        node_missing = node_needs - node_has

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Calculated missing .data files for node {node_rank}: {node_has=}, {node_needs=}, {node_missing=}")
        if node_missing:
            restore_peers = {}
            for missing_hash in node_missing:
                # when restoring from backup the missing hash may not be present in the cluster
                available_from = restore_db.hash_to_node_ranks.get(missing_hash)
                if available_from is not None:
                    available_from_ips = [restore_db.node_rank_to_ip[nr] for nr in available_from]
                    # rotate to the left to spread the load evenly across nodes on restore
                    logging.debug(f"Missing {missing_hash}.data can be obtained from peers: {available_from}, their IPs: {available_from_ips}")
                    restore_peers[missing_hash] = available_from_ips
                    available_from.rotate(-1)
                else:
                    assert restore_db.restore_backup_name, f"Missing {missing_hash}.data is not present in the cluster, and we are not restoring from backup"
                    logging.debug(f"Missing {missing_hash}.data is not present in the cluster and will be fetched from backup '{restore_db.restore_backup_name}'")

            if restore_peers:
                resp["restore-peers"] = restore_peers

    @staticmethod
    def _get_node_peer_ranks(node_rank, restore_db):
        peers_per_node = restore_db.config["peers-per-node"]
        node_count = restore_db.config["nodes"]
        data_parallelism = restore_db.config.get("assume-data-parallelism")

        if data_parallelism:
            dp_partition = node_count // data_parallelism
            peers = set()
            for i in range(peers_per_node):
                # ai is a sequence of -1, 1, -2, 2, ...
                ai = i // 2 + 1
                if i % 2 == 0: ai = -ai
                peers.add((node_rank + ai * dp_partition) % node_count)
        else:
            # first peer is the closest neighbor
            peers = {(node_rank ^ 1) % node_count, }

            # remaining peers are the farthest away on a ring of peers
            dist = node_count // peers_per_node
            for i in range(1, peers_per_node):
                peers.add((node_rank + dist * i) % node_count)

        return list(peers)


def start_server(config, initial_state):
    try:
        Coordinator().start(config, initial_state)
        logging.info(f"Coordinator finished normally")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Coordinator failed: {traceback.format_exc()}")
        sys.exit(1)


coordinator_pid = None


class State(Enum):
    RESTORE = 1
    SYNC = 2
    RESTORE_ONLY = 3  # debug-only state


def init(config, initial_state=State.RESTORE):
    if config["node-rank"] == 0:
        global coordinator_pid
        coordinator_pid = os.fork()
        if coordinator_pid == 0:
            start_server(config, initial_state)

    context = zmq.Context()
    master = context.socket(zmq.REQ)
    coord_address = platform_api.get_coordinator(config["job-name"])
    master.connect(coord_address)
    return master


def stop():
    if coordinator_pid:
        os.kill(coordinator_pid, 9)
        logging.info(f"Coordinator stopped")
