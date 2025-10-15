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
import time

import humanize
import humanize.filesize


class TimeBlock:
    def __init__(self, msg, level=logging.INFO) -> None:
        self.msg = msg
        self.level = level
        self.data = {}

    def __enter__(self):
        logging.log(self.level, f"+ {self.msg}", stacklevel=2)
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.duration = time.time() - self.start_time
        msg = "! " if exc_type else "- "
        msg += self.msg
        self.append_data("duration", self.duration, "s")
        for key, (value, unit, log_rate) in self.data.items():
            val = self._format_value(value, unit, log_rate)
            msg += f"; {key}={val}"

        logging.log(self.level, msg, stacklevel=2)

    def _format_value(self, value, unit, log_rate):
        if isinstance(value, (int, float)):
            if self.duration == 0:
                log_rate = False

            # special cases first
            if unit == "s":
                # for time
                hum = humanize.precisedelta(value)
                hum = f"{value:.2f} s ({hum})"
            elif unit == "B":
                # for size in bytes
                hum = humanize.filesize.naturalsize(value, binary=True)
                if log_rate:
                    r = humanize.filesize.naturalsize(value/self.duration, binary=True)
                    hum += f" @ {r}/s"
            else:
                # for all other numbers
                hum = humanize.intcomma(value)
                if unit != "":
                    hum += " " + unit
                if log_rate:
                    r = humanize.intcomma(value/self.duration, 3)
                    if unit != "":
                        u = f" {unit}/s"
                    else:
                        u = " / s"
                    hum += f" @ {r}{u}"
        else:
            hum = str(value)

        return hum

    def append_msg(self, msg):
        self.msg += f"; {msg}"

    def append_data(self, key, value, unit="", log_rate=False):
        self.data[key] = (value, unit, log_rate)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    with TimeBlock("Hello"):
        time.sleep(1)
