#! /bin/bash

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


set -e -u -x

REPO_ROOT="$(git rev-parse --show-toplevel)"
REGISTRY=${1:?"Error: Specify docker registry path"}
IMAGE_NAME=${2:-"repl"}

REPLICATOR_VERSION="$(hostname)/$(date '+%Y-%m-%d@%H:%M:%S')/$(git rev-parse HEAD)"
if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
    REPLICATOR_VERSION="$REPLICATOR_VERSION.dirty"
fi

docker build -f "$REPO_ROOT/deploy/repl.dockerfile" \
    --build-arg REPLICATOR_VERSION="$REPLICATOR_VERSION" \
    -t "$IMAGE_NAME" \
    "$REPO_ROOT/src/replicator"

docker tag "$IMAGE_NAME" "$REGISTRY/$IMAGE_NAME"
docker push "$REGISTRY/$IMAGE_NAME"
