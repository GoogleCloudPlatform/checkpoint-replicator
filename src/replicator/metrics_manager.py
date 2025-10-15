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

from prometheus_client import Histogram, utils
import threading
import time

NODE_OPERATIONS_SECONDS="node_operations_seconds"

FILE_BUCKETS = (
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 250.0, 500.0, 750.0, 1000.0, 1500.0, #25 min, maximum we can accept
    utils.INF,
)
FILE_BUCKET_TIMEOUT = FILE_BUCKETS[-2] + 10

QUICK_BUCKETS = (
    0.1, 0.5, 1.0, 5.0,
    10.0, 30.0, 40.0, 45.0, 55.0, 75.0, 120.0,
    utils.INF,
)
QUICK_BUCKET_TIMEOUT = QUICK_BUCKETS[-2] + 10
LABELS = ["method_name", "framework", "grpc_status_code"]


class InterimTimerContextManager:
    """
    A context manager that times a block of code.
    - If the execution time exceeds 'interim_threshold', an interim metric (the threshold value) is emitted.
      In this case, the final actual duration is NOT emitted.
    - If the execution time does NOT exceed 'interim_threshold', the final actual duration IS emitted.
      In this case, no interim metric is emitted.
    """

    def __init__(self, histogram_metric: Histogram, interim_threshold: float, actual_labels: dict[str, str]):
        self.histogram_metric = histogram_metric
        self.interim_threshold = interim_threshold
        self._labels = actual_labels

    def __enter__(self):
        self._start_time = time.monotonic()
        # Used to signal completion to the emitter thread
        self._completion_event = threading.Event()
        self._threshold_was_exceeded = False  # Reset flag on entry

        def _interim_emitter_target():
            """
            Target function for the emitter thread.
            Waits for the interim_threshold or completion of the main operation.
            """
            if not self._completion_event.wait(timeout=self.interim_threshold):
                # Timeout occurred: operation is still running past the threshold.
                # print(f"METRICS_DEBUG: '{self.metric_name_for_log}' exceeded {self.interim_threshold}s. Emitting interim metric.")
                self._labels["grpc_status_code"] = "DeadlineExceeded"
                self.histogram_metric.labels(**self._labels).observe(self.interim_threshold)
                self._threshold_was_exceeded = True  # Set flag

        self._emitter_thread = threading.Thread(
            target=_interim_emitter_target, daemon=True)
        self._emitter_thread.start()
        return self  # The context manager returns itself

    def __exit__(self, exc_type, exc_val, exc_traceback):
        duration = time.monotonic() - self._start_time
        self._completion_event.set()

        # Wait for the emitter thread to finish its check, ensuring self.threshold_was_exceeded is correctly set.
        if self._emitter_thread:
            # Join with a timeout slightly larger than the interim
            self._emitter_thread.join(timeout=self.interim_threshold + 0.1)

        # Only observe the final duration if the threshold was NOT exceeded.
        if not self._threshold_was_exceeded:
            if exc_type is not None:
                self._labels["grpc_status_code"] = "Internal"
            else:
                self._labels["grpc_status_code"] = "OK"
            self.histogram_metric.labels(**self._labels).observe(duration)

        return False  # Do not suppress exceptions


class MetricManager:
    _histogram_instances = {}
    _lock = threading.Lock()

    def histogram_client(self, metric_name: str,
                         description: str, bucket, labels=()) -> Histogram:

        with MetricManager._lock:
            if metric_name not in MetricManager._histogram_instances:
                # TODO: put in custom bucket configuration
                MetricManager._histogram_instances[metric_name] = Histogram(
                    name=metric_name, documentation=description, buckets=bucket, labelnames=labels)
            return MetricManager._histogram_instances[metric_name]

    def node_operations_latency(self, actual_labels: dict[str, str]) -> InterimTimerContextManager:
        return self.time_with_interim_emission(NODE_OPERATIONS_SECONDS, "capped latency for replicator operations", FILE_BUCKETS, FILE_BUCKET_TIMEOUT, actual_labels)

    def time_with_interim_emission(self, metric_name: str,
                                   description: str,
                                   bucket,
                                   interim_threshold: float,
                                   actual_labels: dict[str, str]) -> InterimTimerContextManager:
        return InterimTimerContextManager(self.histogram_client(metric_name, description, bucket, labels=LABELS), interim_threshold, actual_labels)


if __name__ == "__main__":
    with MetricManager().time_with_interim_emission(
        "sample_metric", "test metric for functionality", QUICK_BUCKETS, QUICK_BUCKET_TIMEOUT, {"method_name": "test", "framework": "orbax"}):
        time.sleep(1)

    with MetricManager().node_operations_latency({"method_name": "test", "framework": "orbax"}):
        time.sleep(1)