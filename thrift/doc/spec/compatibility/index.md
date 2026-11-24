---
description: Implementation behavior not defined by the Thrift specification
---

# Implementation Compatibility

Behavior not specified as part of the [Thrift specification](../index.md) may
still cause divergent behavior between clients and servers. For example, how a
client sends a frame to close a call. These pages describe what the C++
implementation does in these cases so other implementations can prove
compatibility with it.

[Conformance tests](../../contributions/conformance/index.md) verify this
with `spec/compatibility/` tags.

Note this is not Thrift *schema* compatibility, which covers readers/writers
using different versions of a Thrift schema.

* [Rocket](rocket/index.md)
