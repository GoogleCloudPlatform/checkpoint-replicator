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

import os
import sys

import util


# we start subprocesses that don't inherit globals like config
# but do inherit the environment variables
if util.config:
    _framework = util.config.get("framework")
    os.environ["FRAMEWORK"] = _framework
else:
    _framework = os.getenv("FRAMEWORK")


if _framework == 'pytorch':
    from .pytorch import *
elif _framework == 'pytorch.distributed':
    from .pytorch_distributed import *
elif _framework == 'orbax':
    from .orbax import *
else:
    sys.exit(f"Config param 'framework' is not set to a known value: {_framework}")
