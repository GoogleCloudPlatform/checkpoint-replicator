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

FROM python:3.12.12-slim-bookworm@sha256:874450e1da63e36bf5c1ca8b0a13cb9eb0bbb1c4015151e376978f9aa6ae3bc2

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
