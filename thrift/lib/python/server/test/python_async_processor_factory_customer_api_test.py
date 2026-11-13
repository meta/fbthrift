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


import later.unittest
from thrift.python.test.python_async_processor_factory_customer_api_test import (
    PythonAsyncProcessorFactoryCustomerApiCTest as CTests,
)


class PythonAsyncProcessorFactoryCustomerApiTest(later.unittest.TestCase):
    """Bootstraps component tests for customer-facing Cython APIs."""

    def test_processor_creation_and_legacy_stop_overlap(self) -> None:
        CTests(self).test_processor_creation_and_legacy_stop_overlap()

    def test_set_cpu_worker_threads_rejects_count_below_one(self) -> None:
        CTests(self).test_set_cpu_worker_threads_rejects_count_below_one()

    def test_get_cpu_worker_threads_reports_the_cpp_setting_without_a_handler(
        self,
    ) -> None:
        CTests(
            self
        ).test_get_cpu_worker_threads_reports_the_cpp_setting_without_a_handler()

    async def test_threads_without_factory_context_warn_and_stay_single_threaded(
        self,
    ) -> None:
        await CTests(
            self
        ).test_threads_without_factory_context_warn_and_stay_single_threaded()

    async def test_threads_with_factory_context_serve_requests_on_workers(
        self,
    ) -> None:
        await CTests(self).test_threads_with_factory_context_serve_requests_on_workers()

    async def test_host_without_factory_context_uses_legacy_stop_fallback(
        self,
    ) -> None:
        await CTests(self).test_host_without_factory_context_uses_legacy_stop_fallback()

    async def test_host_with_factory_context_defers_resources_to_exit(
        self,
    ) -> None:
        await CTests(self).test_host_with_factory_context_defers_resources_to_exit()

    async def test_factory_context_composes_handler_context(self) -> None:
        await CTests(self).test_factory_context_composes_handler_context()

    async def test_handler_that_returns_true_still_suppresses_the_callers_exception(
        self,
    ) -> None:
        await CTests(
            self
        ).test_handler_that_returns_true_still_suppresses_the_callers_exception()

    async def test_unary_rpc_round_trips(self) -> None:
        await CTests(self).test_unary_rpc_round_trips()

    async def test_unary_rpc_round_trips_through_composed_factory(self) -> None:
        await CTests(self).test_unary_rpc_round_trips_through_composed_factory()

    async def test_failed_worker_start_propagates_even_when_the_control_handler_suppresses(
        self,
    ) -> None:
        await CTests(
            self
        ).test_failed_worker_start_propagates_even_when_the_control_handler_suppresses()

    async def test_worker_handler_that_lacks_a_routed_method_fails_the_start(
        self,
    ) -> None:
        await CTests(
            self
        ).test_worker_handler_that_lacks_a_routed_method_fails_the_start()

    async def test_worker_product_that_is_not_a_service_interface_fails_the_start(
        self,
    ) -> None:
        await CTests(
            self
        ).test_worker_product_that_is_not_a_service_interface_fails_the_start()

    async def test_worker_shutdown_failure_propagates_when_the_control_handler_does_not_suppress(
        self,
    ) -> None:
        await CTests(
            self
        ).test_worker_shutdown_failure_propagates_and_the_control_handler_exits(None)

    async def test_worker_shutdown_failure_propagates_even_when_the_control_handler_suppresses(
        self,
    ) -> None:
        await CTests(
            self
        ).test_worker_shutdown_failure_propagates_and_the_control_handler_exits(True)

    async def test_failed_worker_start_releases_resources_and_propagates_the_handler_exit_failure(
        self,
    ) -> None:
        await CTests(
            self
        ).test_failed_worker_start_releases_resources_and_propagates_the_handler_exit_failure()
