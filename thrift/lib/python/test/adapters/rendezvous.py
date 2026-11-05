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

"""Field adapter that can hold concurrent field reads at a barrier.

While a test sets `RendezvousAdapter.barrier`, every call that converts a field
value waits there. Threads that read the same uncached field all reach the
barrier before any of them stores its value, so a race to fill the struct field
cache happens on every run, with or without the GIL.
"""

from __future__ import annotations

import threading
import typing

from thrift.python.adapter import Adapter
from thrift.python.types import Struct, StructOrUnion


T = typing.TypeVar("T")


class Box(typing.Generic[T]):
    __slots__ = ["value"]
    on_delete: typing.ClassVar[typing.Callable[[int], None] | None] = None

    def __init__(self, value: T) -> None:
        self.value: T = value

    def __del__(self) -> None:
        on_delete = Box.on_delete
        if on_delete is not None:
            on_delete(id(self))


class RendezvousAdapter(Adapter[T, Box[T]]):
    barrier: typing.ClassVar[threading.Barrier | None] = None
    shared_value: typing.ClassVar[Box[typing.Any] | None] = None

    @classmethod
    def from_thrift_field(
        cls,
        original: T,
        field_id: int,
        strct: StructOrUnion,
        *,
        transitive_annotation: typing.Optional[Struct] = None,
    ) -> Box[T]:
        barrier = cls.barrier
        if barrier is not None:
            barrier.wait()
        shared_value = cls.shared_value
        if shared_value is not None:
            return shared_value
        return Box(original)

    @classmethod
    def to_thrift_field(
        cls,
        adapted: Box[T],
        field_id: int,
        strct: StructOrUnion,
        *,
        transitive_annotation: typing.Optional[Struct] = None,
    ) -> T:
        return adapted.value
