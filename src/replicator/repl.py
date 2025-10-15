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

import sys
import traceback

from common_main import *
import sync
import restore

if __name__ == "__main__":
    try:
        run_args = common_main()
        restore.run(*run_args)
        sync.run(*run_args)
        common_cleanup()
        logging.info(f"Replicator exited normally")
    except Exception as e:
        logging.critical(f"Replicator failed: {traceback.format_exc()}")
        sys.exit(1)
