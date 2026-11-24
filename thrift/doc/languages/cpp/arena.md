---
state: experimental
description: Allocating strings and containers from a memory resource such as an arena
---

# Arena allocation in C++

:::note

`@cpp.Arena` is an experimental feature that is only implemented in C++. Its
generated API may change.

:::

`@cpp.Arena` gives string, binary and container fields
[PMR](https://en.cppreference.com/w/cpp/memory/polymorphic_allocator) types, so
a struct, union or exception can allocate them from a
`std::pmr::memory_resource` such as an arena
(`std::pmr::monotonic_buffer_resource`). Allocating from an arena is cheaper
than from the heap, and the arena frees everything at once when it is
destroyed.

## Annotating fields

```thrift
include "thrift/annotation/cpp.thrift"

struct Order {
  @cpp.Arena
  1: string customer;
  @cpp.Arena
  2: list<string> items;
  3: i64 id;
}

// Applies to each field of Batch that supports it.
@cpp.Arena
struct Batch {
  1: list<Order> orders;
  2: map<string, i64> countByItem;
}
```

A field with `@cpp.Arena` gets the PMR counterpart of its C++ type, and so do
the elements of a container, at every level:

| Field | C++ type |
| --- | --- |
| `string customer` | `std::pmr::string` |
| `list<string> items` | `std::pmr::vector<std::pmr::string>` |
| `map<string, i64> countByItem` | `std::pmr::map<std::pmr::string, std::int64_t>` |
| `list<Order> orders` | `std::pmr::vector<Order>` |

The `std` strings, vectors, deques, lists, sets and maps, including those named
by `@cpp.Type`, map to their `std::pmr` counterparts. `folly::fbvector`, the
F14 sets and maps, and `folly::sorted_vector_set` and
`folly::sorted_vector_map` keep their template and use
`std::pmr::polymorphic_allocator`.

A struct or union field gets `@cpp.Arena` only if its type has `@cpp.Arena`
fields itself; it then keeps its type and is constructed with its parent's
memory resource. A container of structs without `@cpp.Arena` fields puts only
its own memory on the resource: the strings and containers inside those
structs use the default resource.

`@cpp.Arena` on a struct, union or exception applies to each of its fields
that supports it. On a field, it is an error if the field doesn't support it:

* fields with an adapter, or whose type or element type is adapted
* `@cpp.Ref` and `@cpp.Lazy` fields
* `folly::IOBuf` binary fields
* fields with a default value

`@cpp.Arena` on a typedef is an error, and a type with `@cpp.Arena` fields
can't use `cpp.allocator` or `cpp.allocator_via`.

## Using the generated types

A type with `@cpp.Arena` fields is allocator-aware: it has an
`allocator_type` of `std::pmr::polymorphic_allocator<std::byte>`, a
`get_allocator()` method and allocator-extended constructors, which take a
`std::pmr::memory_resource*` as well:

```cpp
// The arena must outlive every value allocated from it, moved-from values
// included.
std::pmr::monotonic_buffer_resource arena;
Order order{&arena};
order.customer() = "Alice";
order.items()->emplace_back("apple");
```

Deserializing into a value allocates its fields from the value's memory
resource:

```cpp
std::pmr::monotonic_buffer_resource arena;
Batch batch{&arena};
apache::thrift::CompactSerializer::deserialize(buf, batch);
```

Like other allocator-aware C++ types:

* Default construction uses `std::pmr::get_default_resource()`.
* Copy construction uses the default resource, not the source's.
* Move construction keeps the source's resource.
* Move assignment and swap keep each value's resource. Across different
  resources, they copy the fields.

## Destruction

When a value's resource is a `std::pmr::monotonic_buffer_resource` (exactly,
not a class derived from it) or derives from `apache::thrift::pmr::ArenaResource`
(`thrift/lib/cpp2/pmr/ArenaResource.h`), its destructor skips the fields that
allocate only from that resource, since the resource frees their memory
anyway. Fields that hold memory from elsewhere, such as a container of structs
without `@cpp.Arena` fields or a `@thrift.Box` field, are still destroyed.

To make sure all the fields of a struct are skipped, check it at compile time:

```cpp
#include <thrift/lib/cpp2/pmr/Teardown.h>

// Fails to build if destroying an `Order` on an arena could free memory that
// the arena doesn't own.
static_assert(
    apache::thrift::pmr::allocate_all_memory_via_memory_resource_v<Order>);
```

Derive a memory resource from `ArenaResource` only if it frees all the memory
it hands out when it is released or destroyed.
