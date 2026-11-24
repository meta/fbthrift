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

"""The schema bundles that a thrift-python service carries.

A generated ``thrift_types`` module built with ``with_schema`` holds its
program's schema in a ``_fbthrift_schema_<hash>`` attribute: zstd-compressed,
Compact-serialized ``apache.thrift.type.schema.Schema``. The server runtime
hands these bundles to C++, which reads them with
``apache::thrift::schema::detail::readSchema`` and builds the ``SyntaxGraph``
that lets ServiceInterceptors see a method's arguments.

One bundle covers one program, so a service needs its own bundle plus those of
every program it references. Those are collected here by walking the generated
import graph: a ``thrift_types`` module imports each dependency's
``thrift_types`` module under a module-level alias, so the dependencies are
reachable from the module's namespace without reading any bundle.
"""

from __future__ import annotations

import importlib
import sys
import types

_SCHEMA_ATTR_PREFIX = "_fbthrift_schema_"
_TYPES_MODULE_SUFFIX = ".thrift_types"
# A mutable handler's interfaces are defined in `thrift_mutable_services`, but
# the bundle lives only in the immutable `thrift_types`:
# `thrift_mutable_types` carries no constants, so no `_fbthrift_schema_*`.
_SERVICES_MODULE_SUFFIXES = (".thrift_services", ".thrift_mutable_services")


def _schema_bundle_of(module: types.ModuleType) -> bytes | None:
    """``module``'s own schema bundle, or None if it carries none."""
    for name, value in vars(module).items():
        if name.startswith(_SCHEMA_ATTR_PREFIX) and isinstance(value, bytes):
            return value or None
    return None


def _collect(
    module: types.ModuleType,
    bundles: list[bytes],
    seen: set[str],
    seen_bundles: set[bytes],
) -> None:
    if module.__name__ in seen:
        return
    seen.add(module.__name__)

    # Two modules can carry the same bundle; merging it twice would collide on
    # its own definition keys.
    bundle = _schema_bundle_of(module)
    if bundle is not None and bundle not in seen_bundles:
        seen_bundles.add(bundle)
        bundles.append(bundle)

    # Keep walking through a module that carries no bundle of its own (a
    # library built without `with_schema`): the modules past it may carry one.
    for value in vars(module).values():
        if isinstance(value, types.ModuleType) and value.__name__.endswith(
            _TYPES_MODULE_SUFFIX
        ):
            _collect(value, bundles, seen, seen_bundles)


def _types_modules_of(handler_class: type) -> list[types.ModuleType]:
    """The generated ``thrift_types`` modules of the service ``handler_class``
    implements and of the services it extends, the service's own first.

    A handler is a user-written subclass of the generated
    ``<Service>Interface``, so its own module is the user's, not the generated
    one. The generated interfaces are found on the MRO instead, by the
    ``thrift_services`` or ``thrift_mutable_services`` modules they are defined
    in; each bundle lives in the immutable ``thrift_types`` module beside them,
    which a mutable handler may not have imported yet.

    The base services' modules are needed on their own: a ``thrift_types``
    module imports only the included programs that have types, so a base
    service from a program without types is not reachable from it.
    """
    modules: list[types.ModuleType] = []
    for klass in handler_class.__mro__:
        module = _types_module_beside(getattr(klass, "__module__", "") or "")
        if module is not None and module not in modules:
            modules.append(module)
    return modules


def _types_module_beside(services_module_name: str) -> types.ModuleType | None:
    """The immutable ``thrift_types`` module of the program whose generated
    services module is ``services_module_name``, or None if that is not a
    generated services module."""
    for suffix in _SERVICES_MODULE_SUFFIXES:
        if services_module_name.endswith(suffix):
            name = services_module_name[: -len(suffix)] + _TYPES_MODULE_SUFFIX
            break
    else:
        return None
    module = sys.modules.get(name)
    if module is not None:
        return module
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


def collect_schema_bundles(handler_class: type) -> list[bytes]:
    """The schema bundles for the service ``handler_class`` implements.

    The service's own bundle comes first, followed by the bundles reachable
    from it and from the services it extends. Returns an empty list when the
    service's Thrift library carries no bundled schema, which leaves its
    generated module without one.
    """
    bundles: list[bytes] = []
    seen: set[str] = set()
    seen_bundles: set[bytes] = set()
    for module in _types_modules_of(handler_class):
        _collect(module, bundles, seen, seen_bundles)
    return bundles
