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

FROM python:3.12.12-slim-bookworm@sha256:78e702aee4d693e769430f0d7b4f4858d8ea3f1118dc3f57fee3f757d0ca64b1

RUN apt update && apt upgrade -y
RUN apt install --fix-missing -y nfs-client host procps

EXPOSE 4242

CMD ["bash", "docker-entrypoint.sh"]

WORKDIR /app
RUN mkdir local repl backup

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY rpc/*.py rpc/
COPY platform_api/*.py platform_api/
COPY framework/*.py framework/
COPY *.py docker-entrypoint.sh ./

ARG REPLICATOR_VERSION
RUN echo "${REPLICATOR_VERSION}" >replicator.ver
