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


import copy
import gc
import pickle
import threading
import unittest
from collections.abc import Callable
from typing import TypeVar

from parameterized import parameterized
from python_test.adapter.thrift_types import FieldCacheRace
from test_thrift.thrift_types import (
    easy,
    Integers,
    Nested1,
    Nested2,
    Nested3,
    NestedStructContainers,
    OptionalColorGroups,
)
from thrift.lib.python.test.testing_utils import run_concurrently
from thrift.python.serializer import deserialize, serialize
from thrift.python.test.adapters.rendezvous import Box, RendezvousAdapter
from thrift.python.types import StructOrUnion


T = TypeVar("T", bound=StructOrUnion)


def serialization_roundtrip(struct: T) -> T:
    data = serialize(struct)
    return deserialize(type(struct), data)


def pickle_roundtrip(struct: T) -> T:
    data = pickle.dumps(struct)
    return pickle.loads(data)


def count_live_objects(cls: type) -> int:
    gc.collect()
    return sum(type(obj) is cls for obj in gc.get_objects())


def race_first_reads_of_field_a(nstructs: int, nthreads: int) -> None:
    structs = [
        Nested1(a=Nested2(b=Nested3(c=easy(val=i, name="foo"))))
        for i in range(nstructs)
    ]

    def worker() -> None:
        for s in structs:
            _ = s.a

    run_concurrently(worker_func=worker, nthreads=nthreads)


class FreeThreading_StructTests(unittest.TestCase):
    def test_struct_init_and_serde(self) -> None:
        def worker() -> None:
            s1 = Nested1(a=Nested2(b=Nested3(c=easy(val=42, name="foo"))))
            s2 = serialization_roundtrip(s1)
            self.assertEqual(s1, s2)

        run_concurrently(worker_func=worker, nthreads=10)

    def test_serde_same_struct_from_multiple_threads(self) -> None:
        def worker(s1: Nested1) -> None:
            s2 = serialization_roundtrip(s1)
            self.assertEqual(s1, s2)

        s1 = Nested1(a=Nested2(b=Nested3(c=easy(val=42, name="foo"))))
        run_concurrently(worker_func=worker, nthreads=10, args=(s1,))

    def test_copy_deepcopy(self) -> None:
        def worker(s1: Nested1) -> None:
            s2 = copy.copy(s1)
            self.assertEqual(s1, s2)
            s3 = copy.deepcopy(s1)
            self.assertEqual(s1, s3)

        s1 = Nested1(a=Nested2(b=Nested3(c=easy(val=42, name="foo"))))
        run_concurrently(worker_func=worker, nthreads=10, args=(s1,))

    def test_call_replace(self) -> None:
        def worker(s1: easy) -> None:
            tid = threading.get_ident() % 1000000000
            s2 = s1(val=tid, name=f"{tid}")
            self.assertEqual(s2.val, tid)
            self.assertEqual(s2.name, f"{tid}")

        s1 = easy(val=1, an_int=Integers(small=300), name="foo")
        run_concurrently(worker_func=worker, nthreads=10, args=(s1,))

    @parameterized.expand(
        [
            (
                "struct",
                lambda i: Nested1(a=Nested2(b=Nested3(c=easy(val=i, name="foo")))),
                "a",
            ),
            (
                "list",
                lambda i: NestedStructContainers(easy_list=[easy(val=i)]),
                "easy_list",
            ),
            (
                "map",
                lambda i: NestedStructContainers(easy_map={"k": easy(val=i)}),
                "easy_map",
            ),
            ("adapted", lambda i: FieldCacheRace(value=Box(str(i))), "value"),
            (
                "optional_set",
                lambda i: OptionalColorGroups(color_list=[i]),
                "color_list",
            ),
            ("optional_unset", lambda i: OptionalColorGroups(), "color_list"),
        ]
    )
    def test_concurrent_first_reads_share_one_field_value(
        self,
        _kind: str,
        make_struct: Callable[[int], StructOrUnion],
        field_name: str,
    ) -> None:
        # GIVEN
        nthreads = 8
        structs = [make_struct(i) for i in range(2000)]
        reads_by_thread: list[list[object]] = []
        reads_lock = threading.Lock()
        expected_divergent_indexes: list[int] = []

        def worker() -> None:
            reads = [getattr(s, field_name) for s in structs]
            with reads_lock:
                reads_by_thread.append(reads)

        # WHEN
        run_concurrently(worker_func=worker, nthreads=nthreads)

        # THEN
        first_reads = reads_by_thread[0]
        actual_divergent_indexes = [
            i
            for i, first_read in enumerate(first_reads)
            if any(reads[i] is not first_read for reads in reads_by_thread)
        ]
        self.assertEqual(expected_divergent_indexes, actual_divergent_indexes)

    def test_cached_read_retains_value_after_struct_release(self) -> None:
        live_boxes_before = count_live_objects(Box)
        struct = FieldCacheRace(value=Box("cached"))
        first_value = struct.value
        barrier = threading.Barrier(2)
        barrier.abort()
        RendezvousAdapter.barrier = barrier
        self.addCleanup(setattr, RendezvousAdapter, "barrier", None)

        cached_value = struct.value
        self.assertIs(first_value, cached_value)
        del first_value, struct
        self.assertEqual("cached", cached_value.value)
        del cached_value
        self.assertEqual(live_boxes_before, count_live_objects(Box))

    def test_cache_miss_retains_nested_value_after_struct_release(self) -> None:
        live_parents_before = count_live_objects(Nested1)
        live_children_before = count_live_objects(Nested2)
        expected_live_children = live_children_before + 1
        expected_leaf_value = 42
        expected_leaf_name = "cached"
        struct = Nested1(a=Nested2(b=Nested3(c=easy(val=42, name="cached"))))

        field_value = struct.a
        del struct

        live_parents_after = count_live_objects(Nested1)
        live_children_after = count_live_objects(Nested2)
        self.assertEqual(live_parents_before, live_parents_after)
        self.assertEqual(expected_live_children, live_children_after)

        actual_leaf_value = field_value.b.c.val
        actual_leaf_name = field_value.b.c.name
        self.assertEqual(expected_leaf_value, actual_leaf_value)
        self.assertEqual(expected_leaf_name, actual_leaf_name)

        del field_value
        live_children_after_value_release = count_live_objects(Nested2)
        self.assertEqual(live_children_before, live_children_after_value_release)

    def test_discarded_field_value_destructor_reenters_cache(self) -> None:
        struct = FieldCacheRace(value=Box("race"))
        reads: list[Box[str]] = []
        reentrant_reads: list[Box[str]] = []
        destroyed_box_ids: list[int] = []
        expected_destructor_calls = 1
        expected_destructor_reads = 1
        expected_cached_value = "race"
        RendezvousAdapter.barrier = threading.Barrier(2, timeout=30)
        self.addCleanup(setattr, RendezvousAdapter, "barrier", None)
        self.addCleanup(setattr, Box, "on_delete", None)

        def reenter(destroyed_box_id: int) -> None:
            destroyed_box_ids.append(destroyed_box_id)
            reentrant_reads.append(struct.value)

        def worker() -> None:
            value = struct.value
            reads.append(value)

        Box.on_delete = reenter
        # Each fieldCacheGetOrCreate call observes an empty slot and starts
        # conversion. The adapter barrier stops both before either creates a
        # Box, so neither can fill the cache before both have observed it empty.
        # Both conversions create a Box after the barrier, even if one finishes first.
        # fieldCacheGetOrCreate caches one Box and releases the other.
        # The discarded Box's __del__ records its identity and reads the cached Box.
        try:
            run_concurrently(worker_func=worker, nthreads=2)
        finally:
            Box.on_delete = None
            RendezvousAdapter.barrier = None

        actual_destructor_calls = len(destroyed_box_ids)
        actual_destructor_reads = len(reentrant_reads)
        self.assertEqual(expected_destructor_calls, actual_destructor_calls)
        self.assertEqual(expected_destructor_reads, actual_destructor_reads)

        cached_box, box_returned_to_other_worker = reads
        box_read_by_destructor = reentrant_reads[0]
        self.assertIs(cached_box, box_returned_to_other_worker)
        self.assertIs(cached_box, box_read_by_destructor)

        cached_box_id = id(cached_box)
        destroyed_box_id = destroyed_box_ids[0]
        self.assertNotEqual(cached_box_id, destroyed_box_id)

        actual_cached_value = box_read_by_destructor.value
        self.assertEqual(expected_cached_value, actual_cached_value)

    def test_failed_field_conversion_leaves_cache_empty(self) -> None:
        struct = FieldCacheRace(value=Box("retry"))
        barrier = threading.Barrier(2)
        barrier.abort()
        RendezvousAdapter.barrier = barrier
        self.addCleanup(setattr, RendezvousAdapter, "barrier", None)

        with self.assertRaises(threading.BrokenBarrierError):
            _ = struct.value

        RendezvousAdapter.barrier = None
        value = struct.value
        self.assertEqual("retry", value.value)
        self.assertIs(value, struct.value)

    def test_forced_first_read_race_keeps_one_value(self) -> None:
        # GIVEN
        struct = FieldCacheRace(value=Box("race"))
        reads: list[Box[str]] = []
        reads_lock = threading.Lock()
        expected_live_boxes = 1
        RendezvousAdapter.barrier = threading.Barrier(2, timeout=30)
        self.addCleanup(setattr, RendezvousAdapter, "barrier", None)

        def worker() -> None:
            value = struct.value
            with reads_lock:
                reads.append(value)

        # WHEN
        run_concurrently(worker_func=worker, nthreads=2)

        # THEN
        actual_same_value = reads[0] is reads[1]
        reads.clear()
        actual_live_boxes = count_live_objects(Box)
        self.assertTrue(actual_same_value)
        self.assertEqual(expected_live_boxes, actual_live_boxes)

    def test_forced_first_read_race_frees_shared_value(self) -> None:
        live_boxes_before = count_live_objects(Box)
        expected_leaked_boxes = 0

        def race() -> None:
            struct = FieldCacheRace(value=Box("race"))
            shared_value = Box("race")
            RendezvousAdapter.shared_value = shared_value
            RendezvousAdapter.barrier = threading.Barrier(2, timeout=30)

            def worker() -> None:
                self.assertIs(struct.value, shared_value)

            try:
                run_concurrently(worker_func=worker, nthreads=2)
            finally:
                RendezvousAdapter.barrier = None
                RendezvousAdapter.shared_value = None

        race()
        actual_leaked_boxes = count_live_objects(Box) - live_boxes_before
        self.assertEqual(expected_leaked_boxes, actual_leaked_boxes)

    def test_concurrent_first_reads_free_discarded_field_values(self) -> None:
        # GIVEN
        rounds = 10
        expected_leaked_nested2 = 0
        live_nested2_before = count_live_objects(Nested2)

        # WHEN
        for _ in range(rounds):
            race_first_reads_of_field_a(nstructs=2000, nthreads=8)
        actual_leaked_nested2 = count_live_objects(Nested2) - live_nested2_before

        # THEN
        self.assertEqual(expected_leaked_nested2, actual_leaked_nested2)

    def test_pickle_same_struct_from_multiple_threads(self) -> None:
        def worker(s1: Nested1) -> None:
            s2 = pickle_roundtrip(s1)
            self.assertEqual(s1, s2)

        s1 = Nested1(a=Nested2(b=Nested3(c=easy(val=42, name="foo"))))
        run_concurrently(worker_func=worker, nthreads=10, args=(s1,))
