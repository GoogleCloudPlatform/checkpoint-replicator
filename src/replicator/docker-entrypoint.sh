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

set -e -u

local_volume="${LOCAL_VOLUME:-local}"

function log_local_storage() {
    echo Volume mounts:
    # Only call df on local mounts (-l); if an NFS server has hung
    # because its node has died, then df will hang. The mount command
    # following will list all mounts with no danger of hanging. The
    # umount -l used in stop(), below, similarly will not hang.
    df -hl
    mount

    echo State of local volume at $local_volume:
    ls -lh "$local_volume"

    echo Disk usage by dirs on $local_volume:
    du -h --max-depth=1 "$local_volume"

    # print some interesting files
    for f in "*.meta" "replicator.*" "jax-init-info.txt"; do
      for m in `compgen -G "$local_volume/$f"`; do
        echo --- $m
        sed 's/^/>> /' $m
        echo
      done
    done
}

function stop() {
    set +e

    echo Replicator Terminating ...

    log_local_storage

    echo "Unmounting repl/*"
    for mnt in repl/*; do
      if ! umount -l "$mnt"; then
        echo "Failed to unmount $mnt, trying force lazy unmount"
        umount -lf "$mnt"
      fi
    done

    rm -rf repl/*

    echo "Exiting ..."
    exit 0
}

trap stop TERM EXIT INT QUIT

# the lifecycle of /home ties with the container, /home should be blank after container restart which is what we want
mkdir -p /home/prometheus_multiprocess
export PROMETHEUS_MULTIPROC_DIR=/home/prometheus_multiprocess

echo Replicator Starting ...
log_local_storage

if [ -f "replicator.ver" ]; then
  echo -n "Replicator version: "
  cat replicator.ver
fi

python3 repl.py
