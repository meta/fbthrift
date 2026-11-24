---
description: Go Thrift implementation and usage
---

# Go

Thrift natively supports Go, with generated types, clients, and server interfaces and a pure-Go RPC runtime. Go clients and servers can communicate with Thrift implementations in other languages.

The [Go runtime](https://www.internalfb.com/code/fbsource/xplat/thrift/lib/go/thrift) is available as the module `github.com/facebook/fbthrift/thrift/lib/go/thrift`. Generate Go code with the compiler's `mstch_go` generator.

## Generated Code

Each Thrift file generates a Go package containing its types, constants, service clients, and server interfaces. Use `namespace go` to specify the import path:

```thrift
namespace go "example.com/project/search"
```

This namespace corresponds to import path `example.com/project/search` and package name `search`. A namespace containing `/` is used as the import path unchanged. Otherwise, every `.` is converted to `/`; for example, `example.search` becomes `example/search`. Use a quoted slash-containing namespace for a domain-qualified import path.

Without an explicit Go namespace, the default is the lowercase Thrift file name without its extension, not a namespace derived from the Thrift `package` declaration.

Use the generated `New<Type>()` constructors to initialize Thrift-defined field defaults. A Go struct literal does not apply those defaults. Thrift exceptions implement Go's `error` interface and are returned as errors from RPC methods. A generated exception's `Error()` method returns the struct's `String()` representation rather than the field annotated with `@thrift.ExceptionMessage`. Undeclared handler errors are sent to the client as `*types.ApplicationException`.

## Serialization and RPC

The `github.com/facebook/fbthrift/thrift/lib/go/thrift/format` package provides Binary and Compact serialization and deserialization through `EncodeBinary`/`DecodeBinary` and `EncodeCompact`/`DecodeCompact`, as well as the deprecated JSON (`EncodeCompactJSON`/`DecodeCompactJSON`) and SimpleJSON (`EncodeSimpleJSON`/`DecodeSimpleJSON`) formats. Go RPC uses Binary or Compact serialization.

The runtime supports Rocket clients and servers, including Header-to-Rocket upgrades for interoperability with legacy peers. Go's Rocket codecs handle ZSTD, ZLIB, and LZ4 compression, but the native client sends requests and sink items uncompressed by default.

Go supports both clients and servers for:

- [Server-to-client streaming](https://www.internalfb.com/code/fbsource/xplat/thrift/lib/go/thrift/docs/streaming.md).
- [Client-to-server streaming (sinks)](https://www.internalfb.com/code/fbsource/xplat/thrift/lib/go/thrift/docs/sink.md).
- [Bidirectional streaming](https://www.internalfb.com/code/fbsource/xplat/thrift/lib/go/thrift/docs/bidi.md).
- [Interaction factories](https://www.internalfb.com/code/fbsource/xplat/thrift/lib/go/thrift/docs/interactions.md). Legacy `performs` interaction constructors are not supported.

## Runtime Metadata

With metadata generation enabled (the default), generated Go packages and service processors expose `GetThriftMetadata()`. Generated structured types expose `GetThriftStructMetadata()`. Treat the returned generated metadata as read-only.

Metadata describes types, fields, functions, and services, including structured annotations on supported declaration scopes. It is not a complete IDL AST: field/parameter/throws annotations and defaults, individual enum-value annotations, full qualifiers, and standalone interaction definitions are not exhaustively represented.

See the [feature comparison](/features/index.md) for feature-specific support and limitations.
