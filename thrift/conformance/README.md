# Thrift conformance tests

Conformance tests verify a language's Thrift implementation against the C++
baseline and report where they align or not.

Generators live in `data/` and write a `TestSuite` to stdout.

Test targets live under `test/`. Each registers a test per-case. Naming is
like `<category>/<suite>/<case>.<test>` ala gtest.

A `<category>` is a language plus any options the test target runs with, e.g.
`rust[client=cpp,protocol=binary]` is the Rust client backed by C++ running
with `--protocol=binary`.

## Running tests

```
# run everything
buck2 test fbcode//thrift/conformance/test/...

# run a subset
buck2 test fbcode//thrift/conformance/test/rpc/rust:rpc-client-test -- --regex SinkAbort
```

## nonconforming.txt

`data/nonconforming.txt` lists the tests **expected** to fail because they do
not conform. These failures are enforced, if a nonconforming test passes that's
a failure too. When nonconforming cases are fixed they should be removed from
`nonconforming.txt`.

A line can glob (`*`, `?`) and admit any options (`[client=cpp,*]`), so to see
exactly which tests it names:

```
# print every nonconforming `java2` test
buck2 run fbsource//xplat/thrift/conformance/data:nonconforming -- java2

# print why a line is nonconforming
buck2 run fbsource//xplat/thrift/conformance/data:nonconforming -- --why rust

# print lines that are nest tests. this builds all conformance targets so
# may take a minute or two.
buck2 run fbsource//xplat/thrift/conformance/data:nonconforming -- --stale
```
