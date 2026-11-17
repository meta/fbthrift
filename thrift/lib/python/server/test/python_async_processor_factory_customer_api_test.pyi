# Copyright (c) Meta Platforms, Inc. and affiliates.
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

# pyre-strict

import unittest

class PythonAsyncProcessorFactoryCustomerApiCTest:
    def __init__(self, unit_test: unittest.TestCase) -> None: ...
    def test_processor_creation_and_legacy_stop_overlap(self) -> None: ...
    def test_set_cpu_worker_threads_rejects_count_below_one(self) -> None: ...
    def test_get_cpu_worker_threads_reports_the_cpp_setting_without_a_handler(
        self,
    ) -> None: ...
    async def test_threads_without_factory_context_warn_and_stay_single_threaded(
        self,
    ) -> None: ...
    async def test_threads_with_factory_context_serve_requests_on_workers(
        self,
    ) -> None: ...
    async def test_host_without_factory_context_uses_legacy_stop_fallback(
        self,
    ) -> None: ...
    async def test_host_with_factory_context_defers_resources_to_exit(
        self,
    ) -> None: ...
    async def test_factory_context_composes_handler_context(self) -> None: ...
    async def test_handler_that_returns_true_still_suppresses_the_callers_exception(
        self,
    ) -> None: ...
    async def test_unary_rpc_round_trips(self) -> None: ...
    async def test_unary_rpc_round_trips_through_composed_factory(self) -> None: ...
    async def test_failed_worker_start_propagates_even_when_the_control_handler_suppresses(
        self,
    ) -> None: ...
    async def test_worker_handler_that_lacks_a_routed_method_fails_the_start(
        self,
    ) -> None: ...
    async def test_worker_product_that_is_not_a_service_interface_fails_the_start(
        self,
    ) -> None: ...
    async def test_worker_shutdown_failure_propagates_and_the_control_handler_exits(
        self, exit_result: bool | None
    ) -> None: ...
    async def test_failed_worker_start_releases_resources_and_propagates_the_handler_exit_failure(
        self,
    ) -> None: ...
