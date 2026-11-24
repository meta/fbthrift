# Schema Compatibility

<!-- https://www.internalfb.com/intern/wiki/Thrift/Thrift_Guide/IDL/SchemaEvolution/?noredirect -->

## **Compatibility between different Thrift Structs (Versioning)**

It is normal to make changes to Thrift files as part of software development.  However, unlike a single program where everything gets rebuilt at once, multiple programs that interact with each other via Thrift get rebuilt at different times.  This requires the ability for different versions of the same Thrift file to be compatible with each other.  The rules for serialization and deserialization described earlier are designed to support this compatibility.

Clearly not all changes to a Thrift struct permit the old and new version to be compatible with each other. For example a change to the type of a field results in incompatible versions.

This section describes when two Thrift structs are compatible with each other - i.e., when it is always possible to serialize from one struct and deserialize into the other without problems.  This section simply clarifies information already presented in the previous sections on **Serialization** and **Deserialization**.  Everything described here can be inferred by reading those sections.

The following scenarios get considered when defining compatibility. Note that not all combinations are currently possible

The following outlines the compatibility for changes to data:

|Change    |Wire compatible[^1] |Code compatible[^2] |Notes    |
|---    |---    |---    |---    |
|Add a field    |yes    |yes    |Dropped by old    |
|Remove a field    |yes    |no    |Dropped by new. Do not give its id to a new field. If the types match, Binary, Compact and JSON readers put old values of the removed field into the new one. Reserve the id with `@thrift.ReserveIds`, and the compiler rejects a field that uses it. For a **required** field, see [caution](#caution_required)    |
|Rename a field    |by id only    |no    |Binary, Compact and JSON protocols write the field id. SimpleJSON and JSON5 write the field name, so a reader with the other name loses the field. See [Serialization by name](#serialization-by-name)    |
|Change a field id    |by name only    |yes    |Binary, Compact and JSON readers drop the field, as for a removed field. Formats that serialize by name keep it. See [Serialization by name](#serialization-by-name)    |
|Change field type    |rarely    |no    |1. Changes between utf-8 encoded string and binary are wire compatible for Binary/Compact protocol. JSON and SimpleJSON in C++ and Go, and SimpleJSON in Rust, write binary as base64, so there the change is not wire compatible. After a change from binary to string, a value that is not utf-8 breaks some readers. Rust and thrift-py-deprecated on Python 3 fail to read the whole struct. Java replaces each bad byte with U+FFFD, or throws if `@compat.Strings{onInvalidUtf8 = Report}` applies. thrift-python and thrift-py3 raise `UnicodeDecodeError` when code reads the field. C++, Go and Hack keep the bytes. <br/>2. Changes between i32 and enum are wire compatible. After a change from i32 to enum, a reader can get a number that the enum does not name. See [Versioning with enumeration types](#versioning-with-enumeration-types) <br/>3. For other changes, a struct reader skips a value with another wire type, and the field keeps its default. Only the HHVM extension reads a value whose integer width changed, for example from `i32` to `i64`. Add a new field instead. See [How to avoid incompatible versions](#how-to-avoid-incompatible-versions) and [Changing the type of a thrift field](/fb/troubleshooting/changing-field-types.md)    |
|Add enum value    |yes    |yes    |An old reader gets a value that it does not know, and Java returns `null` by default. See [Versioning with enumeration types](#versioning-with-enumeration-types)    |
|Remove enum value    |yes    |no    |A new reader can get the removed value from an old writer, and Java returns `null` by default. See [Versioning with enumeration types](#versioning-with-enumeration-types)    |
|Change enum value   |no    |no    |    |
|New enum field with no 0 value    |yes    |yes    |A reader does not get a member of the enum if the data does not have the field. See [Versioning with enumeration types](#versioning-with-enumeration-types)    |
|Default on new non-optional field    |yes    |yes    |Gets new default    |
|Default on new optional field    |yes    |yes    |The compiler rejects it by default, and only some languages leave the field unset. See [caution](#caution_optional_default)    |
|Default changed on a non-optional field    |yes    |no    |A writer sends its own default for a field that the code never set, so a reader gets the writer's default    |
|Default changed on an optional field    |yes    |no    |See [caution](#caution_optional_default)    |
|Constant changed    |yes    |no    |Old constant in old, new constant in new    |
|Required to unspecified    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Unspecified to required    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Optional to unspecified    |yes    |no    |An old reader sees the field as set, also when a new writer never set it. See the reverse change in [caution](#caution_unqualified_to_optional)    |
|Unspecified to optional    |yes    |no    |A new reader sees the field as set, also when an old writer never set it. See [caution](#caution_unqualified_to_optional)    |
|Optional to required    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Required to optional    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Required to terse    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Terse to required    |yes    |no    |A reader that enforces **required** can throw. See [caution](#caution_required)    |
|Optional to terse    |yes    |no    |An old reader sees a field set to the intrinsic default as unset. See [caution](#caution_optional_to_terse)    |
|Terse to optional    |yes    |no    |A new reader sees a field that an old writer set to the intrinsic default as unset. See the reverse change in [caution](#caution_optional_to_terse)    |
|Unspecified to terse    |yes    |no    |If the field has a custom default, an old reader gets it where the writer set the intrinsic default. See [caution](#caution_unqualified_to_terse)    |
|Terse to unspecified    |yes    |no    |If the field gets a custom default, a new reader gets it where an old writer set the intrinsic default. See the reverse change in [caution](#caution_unqualified_to_terse)    |
|Mixin to non-mixin    |yes    |no    |Only code gen changes    |
|Non-mixin to mixin    |yes    |yes    |Only code gen changes    |
|Struct to union    |no    |no    |A new union reader can get two or more fields from an old struct writer. C++, Python, Java and Rust readers then throw. Go keeps all the fields, and so does Hack unless the union is strict    |
|Union to struct    |no    |no    |An old union reader can get two or more fields from a new struct writer, with the same results as in struct to union    |
|Struct to exception    |yes    |yes    |    |
|Exception to struct    |yes    |yes    |    |
|Union to exception     |no    |no    |    |
|Exception to union     |no    |no    |    |
|Non-container to container |no    |no    |    |
|Container to non-container |no    |no    |    |

:::note

Thrift fields using **"[default requiredness](../idl/field-qualifiers#fields-that-dont-have-a-specifier-or-thrifttersewrite)"** are defined as **"unqualified"** in this document.

:::

:::note

At Meta, the [FBTHRIFTCOMPAT](/fb/troubleshooting/linters.md#fbthriftcompat) linter checks each diff. It reports an error for some changes that this table calls wire compatible, such as **optional** to **required**. It does not flag an added or removed field.

:::

### **Serialization by name**

SimpleJSON and JSON5 serialize by name, and so does C++ conversion to and from `folly::dynamic`. Binary, Compact and JSON protocols serialize by id.

When serializing by name, the id of the field is not used, and can be ignored.  For two Thrift struct types **Struct1** and **Struct2** to be compatible in this case, the following conditions must hold:

* If **Struct1** has an **optional** field with name **n** of type **T**, then **Struct2** can have a field with name **n**, but is not required to have a field with this name.  However if the field is present, then its type must be **T** and it should be either **optional** or **unqualified**.
* If **Struct1** has an **unqualified** field with name **n** of type **T**, then **S2** can have a field with name **n**, but is not required to have a field with this name.  However if the field is present, then its type must be **T** (and can be any of **required**, **optional**, or **unqualified**).

When a reader deserializes by name, it skips a field name that its struct does not have. The field then keeps its default value. Readers in C++, thrift-python, thrift-py-deprecated, Java and Rust raise no error. The Go reader returns an error, and C++ `from_dynamic()` throws for a union. So after a rename, a reader with the other definition loses the field. Every rollout order leaves a time when one side loses it.

### **Serialization by id**

When serializing by id, it is the id of the fields in the two types that must match, not their names.

For two Thrift struct types **Struct1** and **Struct2** to be compatible when serializing by id, the following conditions must hold:

* If **Struct1** has an **optional** field with id **i** of type **T**, then **Struct2** can have a field with id **i**, but is not required to have a field with this id.  However if the field is present, then its type must be **T** and it should be either **optional** or **unqualified**.
* If **Struct1** has an **unqualified** field with id **i** of type **T**, then **Struct2** can have a field with id **i**, but is not required to have a field with this id.  However if the field is present, then its type must be **T** (and can be any of **required**, **optional**, or **unqualified**).

### **Consequence of incompatible field**

The incompatible field will be dropped during deserialization. There will be temporary data loss until the new version is pushed everywhere.

### **How to avoid incompatible versions**

Instead of modifying the name/id and type, it's recommended to

1. Create a new **optional** field with desired name/id and type, because old writers do not set it
2. Modify writer to write to both field
3. Push service with new changes to the fleet
4. Remove old field, and reserve its id with `@thrift.ReserveIds`

e.g., for the following struct

```
struct Foo {
  1: i32 field;
}
```
We want to change type of `Foo.field` from `i32` to `i64`. If we change it directly, deserializer will drop this field due to type mismatches. To avoid this problem, it's recommended to create a new field first, then double writes the data to both field before deleting the old field.

```
struct Foo {
  1: i32 field;
  2: optional i64 new_field;
}
```
### **Changes that maintain compatibility**

Given the rules above, the following changes to Thrift structs will maintain compatibility between old and new versions:

* Changing a **required** field to **unqualified** (see [caution](#caution_required) below)
* Changing a **required** field to **terse** (see [caution](#caution_required) below)
* Changing an **optional** field to **unqualified**
* Changing an **unqualified** field to **required** (see [caution](#caution_required) below)
* Changing an **unqualified** field to **optional** (see [caution](#caution_unqualified_to_optional) below)
* Changing an **unqualified** field to **terse** (see [caution](#caution_unqualified_to_terse) below)
* Changing a **terse** field to **unqualified**
* Changing a **terse** field to **optional**
* Changing an **optional** field to **terse** (see [caution](#caution_optional_to_terse) below)
* Deleting an **optional** field
* Deleting an **unqualified** field
* Adding a new **optional** field
* Adding a new **unqualified** field
* Adding a new **terse** field
* Renaming a field but keeping its field id the same (only when serializing by field id)
* Changing a field id but keeping its name the same (only when serializing by name)

:::caution

<a id="caution_unqualified_to_optional"></a>The **unqualified** to **optional** migration is compatible from the protocol standpoint, but it is not safe:

* An old writer always writes an **unqualified** field, also when its code never set it. So a new reader sees the field as set.
* In C++, `value()`, `*` and `->` on an unset **optional** field throw `apache::thrift::bad_optional_field_access`. An unset field compares like `std::nullopt`, so `== 0` is false. After the migration, a writer leaves out a field that C++ code cleared with `apache::thrift::unset_unsafe()`.
* In Java, `int c = s.getCount()` still compiles because of autoboxing. But it throws `NullPointerException` when the field is unset.
* Rust and Go code that uses the field as a plain value does not compile. In thrift-python, an unset field reads as `None`.

:::

:::caution

<a id="caution_unqualified_to_terse"></a>The **unqualified** to **terse** migration is compatible from the protocol standpoint. But it changes the values that readers get if the field has a [custom default value](./#pre-defined-value). A **terse** field cannot have a custom default, so the migration removes it.

* A new writer leaves out a **terse** field that holds the intrinsic default, for example 0. An old reader then puts its custom default in the missing field. So a 0 that the new writer set arrives at the old reader as the custom default.
* A new reader puts the intrinsic default in a missing field, where an old reader put the custom default.
* Go, thrift-py-deprecated and `java_deprecated` have no terse write, so their writers still write the field.
* `@thrift.TerseWrite` on a package or a struct makes every **unqualified** field in it **terse**.
* In C++, code that calls `is_set()`, `ensure()` or the deprecated getter or setter of the field no longer compiles. In Java, a string, binary, container or struct field starts empty instead of `null`. A `null` value makes the write throw.

:::

:::caution

<a id="caution_optional_to_terse"></a>The **optional** to **terse** migration is compatible from the protocol standpoint. But a **terse** field cannot tell an unset field from one that holds the [intrinsic default](./#intrinsic-default-values). `@thrift.TerseWrite` is an error on an **optional** field, so the migration removes `optional`.

* A new writer leaves out a field set to the intrinsic default, for example after `s.intField() = 0`. An old reader sees that field as unset, and C++ `value()` on it throws `apache::thrift::bad_optional_field_access`.
* A new Go writer writes the field also when it was never set. An old reader then sees the field as set to the intrinsic default.
* Code that checks whether the field is set breaks. It does not compile in C++ (`has_value()`, `reset()`), Rust, Go, or Java for a number field (`== null`). In thrift-python, the field never reads as `None` again.

:::

:::caution

<a id="caution_required"></a>Changes to or from **required** are compatible from the protocol standpoint only if no reader enforces **required**. Most languages ignore `required`, and the compiler warns about it. Three readers still throw if the data does not have a **required** field:

* `java_deprecated`
* C++ code generated with the `deprecated_enforce_required` option
* Hack `readFromJson()`

A reader that enforces **required** breaks when a writer leaves out the field:

* An **optional** writer leaves out a field that is not set.
* A **terse** writer leaves out a field that holds the intrinsic default.
* An **unqualified** writer leaves out a field that holds `null`. In Java, `java_deprecated` and thrift-py-deprecated, a string, container or struct field with no default is `null` until the code sets it.

A `java_deprecated` writer also throws if a **required** string, binary, container, struct or enum field holds `null`.

:::

:::caution

<a id="caution_optional_default"></a>The compiler rejects a default on an **optional** field unless the field has `@thrift.AllowUnsafeOptionalCustomDefaultValue`. With that annotation, languages do not agree on what the default means:

* C++, thrift-python, Java and Rust start the field unset. If the data does not have the field, a reader sees it as unset.
* Go, Hack, thrift-py-deprecated and `java_deprecated` put the default in the field. So a reader gets the default also when the data does not have the field.
* Go and Hack writers send the default when the code never set the field. So do the accelerated thrift-py-deprecated writers, and `java_deprecated` for a string, binary, container, struct or enum field.
* The thrift-py3 getter returns the default for an unset field. Rust with the `deprecated_optional_with_default_is_some` option works like Go.

:::

### **A degenerate example**

Consider the following degenerate example where the id's are swapped in the two structs:

```
struct Struct1 {
  1: i32 x,
  2: i32 y,
}

struct Struct2 {
  2: i32 x,
  1: i32 y,
}
```
When serializing by field name, the field ids are ignored.  Field `x` in `Struct1` is copied to field `x` in `Struct2`, and field `y` in `Struct1` is copied to field `y` in `Struct2`.

When serializing by field id, field `x` in `Struct1` is copied to field `y` in `Struct2`, and field `y` in `Struct1` is copied to field `x` in `Struct2`.

I.e., `Struct1` and `Struct2` are compatible regardless of the kind of serialization used, but the results are very different based on the kind of serialization used.

:::note

Generally changes that maintain compatibility when serializing by field id do not necessarily maintain compatibility when serializing by field name.

:::

:::note

If you want to delete a **required** field from a Thrift struct, you can do it in two steps: First change the field from **required** to **unqualified**.  Then, once you are sure that all usage of the older version (with **required**) have been updated to use the new version, then you can make the second change to delete the field.

:::

### **Versioning with enumeration types**

When a Thrift struct contains fields with enumeration types, the following modifications to the enumeration types maintain compatibility:

* Deleting an enumerator
* Adding a new enumerator
* Renaming an enumerator but keeping its integer value the same (only when serializing by field id)

After you add an enumerator, a reader with the old definition can get a value that it does not know. After you delete one, a reader with the new definition can get it from an old writer. Each runtime handles the unknown value in its own way:

* C++ keeps the number, and the value has no name. `apache::thrift::util::enumName()` returns `nullptr` for it, and `enumNameOrThrow()` throws.
* thrift-python and thrift-py3 return a `BadEnum`, which is not a member of the enum class.
* Java returns `null`. An enum with `@java.UseIntrinsicDefault` returns its member with value 0 instead. An enum with `@java.OpenEnum` returns `UNRECOGNIZED`, which keeps the number.
* Hack keeps the number from Binary and Compact, and `readFromJson()` sets the field to `null`.
* thrift-py-deprecated keeps the number. Its `readFromJson()` raises `TProtocolException` if the caller passes `relax_enum_validation=False` or `wrap_enum_constants=True`.

So add an enumerator to every reader before any writer sends it. Delete one only after no writer sends it. If you cannot make sure of that, add a new enum and a new field instead.

A new enum field has a similar problem if its enum has no value 0. If the data does not have the field, a reader does not get a member of the enum:

* C++, Go and Rust get 0. Rust gets `i32::MIN` if the code was generated with the `deprecated_default_enum_min_i32` option.
* thrift-python and thrift-py3 return a `BadEnum`.
* Java, `java_deprecated` and Hack return `null`, and thrift-py-deprecated returns `None`.

If the code never set the field, a C++, thrift-python, thrift-py3, Java, Go or Rust writer sends 0. Rust with that option sends `i32::MIN`. So make the new field **optional**, or give it a default. A value 0 in the enum does not help `java_deprecated`, Hack and thrift-py-deprecated, which still return `null` or `None`. It also does not help Rust with that option.

[^1]: Whether the data is changed over the wire.
[^2]: Whether the code change is needed.
