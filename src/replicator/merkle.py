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

from blake3 import blake3
import os
from metrics_manager import QUICK_BUCKETS, MetricManager
from time_block import TimeBlock
from util import file_digest


def dir_digest(path):
    """Compute a digest of a directory tree.

    This algorithm ensures that the digest of a directory is:
    - Order-independent: The order in which the entries are processed does not affect the final digest.
    - Filename-independent: Only the content of the files is hashed, not the directory structure itself.
    - Content-dependent: Any change in the content of the directory or its subdirectories will result in a different digest.

    The algorithm is as follows:
    1. For each entry in the directory:
        a. If the entry is a directory, recursively compute its digest.
        b. If the entry is a file, compute its digest.
    2. Compute the digest of sorted digests of all children, separated by newlines.
    3. Prepend "D" to the digest to distinguish dir and file digests. E.g. thanks to this the digests of an empty file and an empty dir are different.
    """

    with MetricManager().histogram_client("duration_hashing_dir", "time to hash a directory", QUICK_BUCKETS).time(), TimeBlock(f"Hashing dir {path}") as tb:
        children = []
        total_size = 0

        for entry in sorted(os.listdir(path)):
            entry_path = os.path.join(path, entry)

            if os.path.isdir(entry_path):
                hash, size = dir_digest(entry_path)
            else:
                hash, size = file_digest(entry_path)

            total_size += size
            children.append(hash)

        # sort children to make the digest order-independent
        children.sort()

        # join children strings with "\n" and digest using blake3
        hash = blake3("\n".join(children).encode("utf-8")).hexdigest()
        # prepend "D" to distinguish dir and file digests
        hash = "D" + hash

        tb.append_data("blake3_hash", hash)
        tb.append_data("dir_size", total_size, "B", log_rate=True)

    return hash, total_size


if __name__ == "__main__":
    from sys import argv
    import util

    util.init_logging()
    dir = argv[1] if len(argv) > 1 else "."
    dir_digest(dir)
