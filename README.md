Facebook Thrift
---------------

[![Linux Build Status](
  https://github.com/facebook/fbthrift/actions/workflows/linux.yml/badge.svg)](
  https://github.com/facebook/fbthrift/actions/workflows/linux.yml)
[![macOS Build Status](
  https://github.com/facebook/fbthrift/actions/workflows/macos.yml/badge.svg)](
  https://github.com/facebook/fbthrift/actions/workflows/macos.yml)
[![Windows Build Status](
  https://github.com/facebook/fbthrift/actions/workflows/windows.yml/badge.svg)](
  https://github.com/facebook/fbthrift/actions/workflows/windows.yml)
[![Python Build Status](
  https://github.com/facebook/fbthrift/actions/workflows/getdeps_python_linux_container.yml/badge.svg)](
  https://github.com/facebook/fbthrift/actions/workflows/getdeps_python_linux_container.yml)
[![Python Build Status (no container)](
  https://github.com/facebook/fbthrift/actions/workflows/getdeps_python_linux.yml/badge.svg)](
  https://github.com/facebook/fbthrift/actions/workflows/getdeps_python_linux.yml)
<img src="thrift/doc/logo.svg"
     alt="Facebook Thrift Logo" align="right" width="20%"/>

Thrift is a serialization and RPC framework for service communication. Thrift enables these features in all major languages, and there is strong support for C++, Python, Hack, and Java. Most services at Facebook are written using Thrift for RPC, and some storage systems use Thrift for serializing records on disk.

Facebook Thrift is not a distribution of [Apache Thrift](https://thrift.apache.org/). This is an evolved internal branch of Thrift that Facebook re-released to the open source community in February 2014. Facebook Thrift was originally released closely tracking Apache Thrift but is now evolving in new directions. In particular, the compiler was rewritten from scratch and the new implementation features a fully asynchronous Thrift server. Read more about these improvements in the [ThriftServer documentation](https://github.com/facebook/fbthrift/blob/main/thrift/doc/languages/cpp/cpp2.md).

You can also learn more about this project in the original Facebook Code [blog post](https://code.facebook.com/posts/1468950976659943/under-the-hood-building-and-open-sourcing-fbthrift/).

Table of Contents
=========================

* [About Thrift](#about-thrift)
  * [A Code Generator](#a-code-generator)
  * [A Serialization Framework](#a-serialization-framework)
  * [An RPC Framework](#an-rpc-framework)
* [Building](#building)
  * [Build with CMake](#build-with-cmake)
  * [getdeps.py](#getdepspy)
  * [Thrift Files](#thrift-files)
  * [Python Build (thrift-python)](#python-build-thrift-python)
* [C++ Static Reflection](#c-static-reflection)
* [C++ Server Metrics](#c-server-metrics)

## About Thrift
At a high level, Thrift is three major things:

### A Code Generator

Thrift has a code generator which generates data structures that can be serialized using Thrift, and client and server stubs for RPC, in different languages.

### A Serialization Framework

Thrift has a set of protocols for serialization that may be used in different languages to serialize the generated structures created from the code generator.

### An RPC Framework

Thrift has a framework to frame messages to send between clients and servers and to call application-defined functions when receiving messages in different languages.

There are several key goals for these components:
* Ease of use:
  Thrift takes care of the boilerplate of serialization and RPC and enables the developer to focus on the schema of the system's serializable types and on the interfaces of the system's RPC services.

* Cross-language support:
  Thrift enables intercommunication between different languages. For example, a Python client communicating with a C++ server.

* Performance:
  Thrift structures and services enable fast serialization and deserialization, and its RPC protocol and frameworks are designed with performance as a feature.

* Backwards compatibility:
  Thrift allows fields to be added to and removed from serializable types in a manner that preserves backward and forward compatibility.

## Building

### Build with CMake

The CMake build fetches any dependency it cannot find installed, so a clone and
two commands are enough:

    git clone https://github.com/facebook/fbthrift
    cd fbthrift
    cmake -B _build
    cmake --build _build

This needs CMake 3.24 or newer and a C++20 compiler. Everything else, including
zlib, [Zstd](https://github.com/facebook/zstd),
[Folly](https://github.com/facebook/folly),
[Fizz](https://github.com/facebookincubator/fizz),
[Wangle](https://github.com/facebook/wangle),
[mvfst](https://github.com/facebook/mvfst) and their dependencies, is used
as-is if already on the system and fetched otherwise. On Windows, libsodium has
to be installed too. Alternatively, install the system packages first:

    ./build/fbcode_builder/getdeps.py install-system-deps --recursive fbthrift

The build puts its outputs in `_build`:

* `_build/bin/thrift`: The Thrift compiler binary to generate client and
  server code.
* `_build/lib/libthriftcpp2.a`: Library for clients and servers.

Tests are not built by default. Configure with `-DTHRIFT_TESTS=ON` to build
them. They need [GTest](https://github.com/google/googletest) installed; it is
not fetched:

    cmake -B _build -DTHRIFT_TESTS=ON
    cmake --build _build
    (cd _build && ctest)

If configure had to fetch zlib, Zstd, gflags, Folly, Fizz, Wangle, mvfst or
xxHash, it turns install rules off, because an installed Thrift would refer to
libraries that exist only in the build tree. The configure output lists them;
install them and reconfigure to get an installable Thrift.

CMake options:

* `THRIFT_RPC` (default `ON`): build the RPC transport stack and its
  dependencies (Fizz, Wangle, mvfst).
* `THRIFT_TESTS` (default `OFF`): build the tests.
* `THRIFT_BENCHMARKS` (default: the value of `THRIFT_TESTS`): build the
  benchmarks.
* `THRIFT_PYTHON` (default `OFF`): build the thrift-python library and
  extensions; requires Cython.

### `getdeps.py`

`getdeps.py` also builds Thrift's sibling projects and is what the Python CI
uses; the Linux, macOS and Windows CI use the CMake build above. Prefer the
CMake build unless you need that.

    # Build, using system dependencies if available
    ./build/fbcode_builder/getdeps.py --allow-system-packages build fbthrift

`getdeps.py` will invoke cmake etc and put output in its scratch area (you can
see in logs, and can override with `--scratch-path`), including
`installed/fbthrift/bin/thrift` and `installed/fbthrift/lib/libthriftcpp2.a`.

If you want to invoke `cmake` again to iterate, there is a helpful
`run_cmake.py` output in the scratch `build/fbthrift` directory.

### Thrift Files

To generate code from `.thrift` files in a CMake project, find the installed
Thrift, which provides `thrift_generate`:

    find_package(thrift CONFIG REQUIRED)

    add_library(example)
    thrift_generate(example if/example.thrift LANGUAGE cpp2)
    target_include_directories(example PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries(example PUBLIC thrift::protocol)

`thrift_generate` adds the generated sources to `example`; the comment above it
in `ThriftGenerate.cmake` describes the other options. Code generated for
services also needs `thrift::rpc`. A project that adds Thrift with
`add_subdirectory()` or FetchContent uses the same `thrift::` names.

### Python Build (thrift-python)

Build and install the thrift-python wheel for Python development.

#### Container Build with Docker BuildKit (Recommended)

```bash
docker buildx build -t fbthrift-python-build -f .devcontainer/Containerfile .
docker run -it -v $(pwd):/fbthrift -w /fbthrift fbthrift-python-build bash
```

#### Build and Test Commands

```bash
apt-get update
python3 build/fbcode_builder/getdeps.py install-system-deps --recursive fbthrift-python
python3 build/fbcode_builder/getdeps.py build fbthrift-python
python3 build/fbcode_builder/getdeps.py test fbthrift-python
pip3 install $(python3 build/fbcode_builder/getdeps.py show-inst-dir fbthrift-python)/share/thrift/wheels/thrift-*.whl
python3 -c "from thrift.python.types import StructMeta; print('thrift-python installed successfully')"
```

**Note:** The wheel is portable and can be installed into any compatible Python environment (Python 3.8+).

#### Other Ways to Build

**Non-Container Build:**

```bash
sudo apt-get update
sudo apt-get install -y python3-dev patchelf
pip3 install cython auditwheel
python3 build/fbcode_builder/getdeps.py --allow-system-packages install-system-deps --recursive fbthrift-python
python3 build/fbcode_builder/getdeps.py --allow-system-packages build fbthrift-python
python3 build/fbcode_builder/getdeps.py --allow-system-packages test fbthrift-python
```

**Build with Conda:**

```bash
conda create -n thrift-python-build python=3.10
conda activate thrift-python-build
conda install cython=3.2.2
pip3 install auditwheel cmake==3.28.0
sudo apt-get update
sudo apt-get install -y patchelf
python3 build/fbcode_builder/getdeps.py --allow-system-packages install-system-deps --recursive fbthrift-python
python3 build/fbcode_builder/getdeps.py --allow-system-packages build fbthrift-python
python3 build/fbcode_builder/getdeps.py --allow-system-packages test fbthrift-python
```

**Building Containers with Other Runtimes:**

Legacy Docker:
```bash
docker build -t fbthrift-python-build -f .devcontainer/Containerfile .
docker run -it -v $(pwd):/fbthrift -w /fbthrift fbthrift-python-build bash
```

Podman:
```bash
podman build -t fbthrift-python-build -f .devcontainer/Containerfile .
podman run -it -v $(pwd):/fbthrift -w /fbthrift fbthrift-python-build bash
```

**Install the wheel** (for all non-container builds):

```bash
pip3 install $(python3 build/fbcode_builder/getdeps.py show-inst-dir fbthrift-python)/share/thrift/wheels/thrift-*.whl
```

## C++ Static Reflection

Information regarding C++ Static Reflection support can be found under the [static reflection library directory](thrift/lib/cpp2/reflection/), in the corresponding [`README` file](thrift/lib/cpp2/reflection/README.md).

## C++ Server Metrics

To collect runtime stats from a Thrift server, e.g. the number of active requests/connections, the C++ Thrift server supports an observer API that installs callbacks at a set of specific execution points in the server.

To expose collected metrics out of the server process, one way is to use `fb303` interfaces, see [fb303 Github repo](https://github.com/facebook/fb303).

## License

Facebook Thrift is [Apache 2.0 licensed](LICENSE).
