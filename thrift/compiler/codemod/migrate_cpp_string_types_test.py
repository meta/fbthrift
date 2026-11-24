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

# pyre-unsafe

import os
import shutil
import tempfile
import textwrap
import unittest

import pkg_resources
from xplat.thrift.compiler.codemod.test_utils import read_file, run_binary, write_file


class MigrateCppStringTypesTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(tmp)
        self.maxDiff = None

        write_file(
            "thrift/annotation/cpp.thrift",
            textwrap.dedent(
                """\
                package "facebook.com/thrift/annotation/cpp"

                struct Type {
                    1: string name;
                    2: string template;
                }
                struct Adapter {
                    1: string name;
                }
                """
            ),
        )

    def test_migrates_string_types_selected_by_input(self):
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"

            namespace cpp2 example

            @cpp.Type{name = "folly::fbstring"}
            typedef binary Bytes

            @cpp.Type{name = "::apache::thrift::ManagedStringViewWithConversions"}
            typedef string ManagedString

            struct Record {
              @cpp.Type{name = "::facebook::admarket::MicroString"}
              1: string label;

              @cpp.Type{name = "folly::IOBuf"}
              2: binary chained;

              @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
              3: binary chained_ptr;

              @cpp.Type{name = "::folly::IOBuf"}
              7: binary qualified_chained;

              @cpp.Type{name = "::std::unique_ptr< ::folly::IOBuf >"}
              8: binary qualified_chained_ptr;

              @cpp.Type{name = "CustomString"}
              4: string custom;

              @cpp.Adapter{name = "ExistingAdapter"}
              @cpp.Type{name = "AlreadyAdaptedString"}
              5: string already_adapted;

              @cpp.Type{name = "QualifiedString", template = "OtherTemplate"}
              6: string type_with_extra_key;
            }
            """
        )
        expected = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"

            namespace cpp2 example

            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<folly::fbstring>"}
            typedef binary Bytes

            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<::apache::thrift::ManagedStringViewWithConversions>"}
            typedef string ManagedString

            struct Record {
              @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<::facebook::admarket::MicroString>"}
              1: string label;

              @cpp.Type{name = "folly::IOBuf"}
              2: binary chained;

              @cpp.Type{name = "std::unique_ptr<folly::IOBuf>"}
              3: binary chained_ptr;

              @cpp.Type{name = "::folly::IOBuf"}
              7: binary qualified_chained;

              @cpp.Type{name = "::std::unique_ptr< ::folly::IOBuf >"}
              8: binary qualified_chained_ptr;

              @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}
              4: string custom;

              @cpp.Adapter{name = "ExistingAdapter"}
              @cpp.Type{name = "AlreadyAdaptedString"}
              5: string already_adapted;

              @cpp.Type{name = "QualifiedString", template = "OtherTemplate"}
              6: string type_with_extra_key;
            }
            """
        )

        write_file("foo.thrift", before)
        binary = pkg_resources.resource_filename(__name__, "codemod")
        run_binary(binary, "foo.thrift")
        self.assertEqual(read_file("foo.thrift"), expected)

        run_binary(binary, "foo.thrift")
        self.assertEqual(read_file("foo.thrift"), expected)

    def test_iobuf_tokens_and_parameter_metadata(self):
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            include "thrift/annotation/thrift.thrift"

            @cpp.Type{name = "apache::thrift::IOBufChain"}
            typedef binary Chain
            @cpp.Type{name = " ::apache :: thrift :: IOBufChain "}
            typedef string SpacedChain
            @cpp.Type{name = " ::std :: unique_ptr < ::folly :: IOBuf > "}
            typedef binary Ptr
            @cpp.Type{name = "folly::IO Buf"}
            typedef string InvalidIdentifier

            service Api {
              void call(
                @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
                @cpp.Type{name = "CustomString"}
                1: string arg,
                @cpp.Type{name = "apache::thrift::IOBufChain"}
                2: binary chain,
              );
            }
            """
        )
        expected = before.replace(
            '@cpp.Type{name = "folly::IO Buf"}',
            '@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<folly::IO Buf>"}',
        ).replace(
            '@cpp.Type{name = "CustomString"}',
            '@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}',
        )
        self.check_transform(before, expected)

    def test_py3_lifts_fields_and_preserves_allocator_annotations(self):
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            include "thrift/annotation/thrift.thrift"
            namespace py3 example

            typedef string StringType_label
            @cpp.Type{name = "CustomString"}
            @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
            typedef string Custom
            struct Record {
              @cpp.Type{name = "CustomString"}
              @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
              1: optional string label;
              @cpp.Type{name = "CustomString"}
              2: string same;
              @cpp.Type{name = "CustomString"}
              3: Custom redundant;
            }
            """
        )
        expected = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            include "thrift/annotation/thrift.thrift"
            include "thrift/annotation/python.thrift"
            namespace py3 example


            @python.Py3EnableCppAdapter
            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}
            typedef string StringType_label_2

            typedef string StringType_label
            @python.Py3EnableCppAdapter
            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}
            @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
            typedef string Custom
            struct Record {
              
              @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
              1: optional StringType_label_2 label;
              
              2: StringType_label_2 same;
              
              3: Custom redundant;
            }
            """
        )
        self.check_transform(before, expected)

    def test_skips_inherited_annotations_and_customized_aliases(self):
        write_file(
            "types.thrift",
            textwrap.dedent(
                """\
                include "thrift/annotation/cpp.thrift"
                include "thrift/annotation/scope.thrift"
                @scope.Transitive
                @scope.Field
                @cpp.Type{name = "CustomString"}
                struct StringType {}
                """
            ),
        )
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            include "types.thrift"
            @cpp.Adapter{name = "OtherAdapter"}
            typedef string Adapted
            struct Record {
              @types.StringType
              1: string inherited;
              @cpp.Type{name = "CustomString"}
              2: Adapted customized_alias;
            }
            """
        )
        self.check_transform(before, before)

    def test_removes_redundant_overrides_before_and_after_include_migration(self):
        for annotation in (
            '@cpp.Type{name = "CustomString"}',
            '@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}',
        ):
            with self.subTest(annotation=annotation):
                write_file(
                    "types.thrift",
                    'include "thrift/annotation/cpp.thrift"\n'
                    + annotation
                    + "\ntypedef string Text\n",
                )
                before = textwrap.dedent(
                    """\
                    include "thrift/annotation/cpp.thrift"
                    include "types.thrift"
                    struct Record {
                      @cpp.Type{name = "CustomString"}
                      1: types.Text redundant;
                    }
                    """
                )
                expected = before.replace('@cpp.Type{name = "CustomString"}', "")
                self.check_transform(before, expected)

    def test_py3_insertion_preserves_first_definition_annotations(self):
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            include "thrift/annotation/thrift.thrift"
            namespace py3 example

            @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.allocator": "Allocator"}}
            struct First {}
            struct Record {
              @cpp.Type{name = "CustomString"}
              1: string text;
            }
            """
        )
        generated = textwrap.dedent(
            """\

            @python.Py3EnableCppAdapter
            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}
            typedef string StringType_text

            """
        )
        expected = (
            before.replace(
                'include "thrift/annotation/thrift.thrift"\n',
                'include "thrift/annotation/thrift.thrift"\n'
                'include "thrift/annotation/python.thrift"\n',
            )
            .replace(
                "@thrift.DeprecatedUnvalidatedAnnotations",
                generated + "@thrift.DeprecatedUnvalidatedAnnotations",
                1,
            )
            .replace('@cpp.Type{name = "CustomString"}', "")
            .replace("1: string text", "1: StringType_text text")
        )
        self.check_transform(before, expected)

    def test_py3_preserves_plain_aliases(self):
        before = textwrap.dedent(
            """\
            include "thrift/annotation/cpp.thrift"
            namespace py3 example
            typedef string Plain
            struct Record {
              @cpp.Type{name = "CustomString"}
              1: Plain text;
            }
            """
        )
        generated = textwrap.dedent(
            """\

            @python.Py3EnableCppAdapter
            @cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<CustomString>"}
            typedef Plain StringType_text

            """
        )
        expected = (
            before.replace(
                'include "thrift/annotation/cpp.thrift"\n',
                'include "thrift/annotation/cpp.thrift"\n'
                'include "thrift/annotation/python.thrift"\n',
            )
            .replace("typedef string Plain", generated + "typedef string Plain")
            .replace('@cpp.Type{name = "CustomString"}', "")
            .replace("1: Plain text", "1: StringType_text text")
        )
        self.check_transform(before, expected)

    def test_preserves_pmr_string_types_byte_for_byte(self):
        for name in (
            "std::pmr::string",
            "::std::pmr::string",
            " std :: pmr :: string ",
            " ::std :: pmr :: string ",
        ):
            with self.subTest(name=name):
                before = textwrap.dedent(
                    """\
                    include "thrift/annotation/cpp.thrift"
                    include "thrift/annotation/thrift.thrift"
                    namespace py3 example
                    @cpp.Type{name = "PMR_TYPE"}
                    typedef string Text
                    @cpp.Type{name = "PMR_TYPE"}
                    typedef binary Bytes
                    struct Record {
                      @cpp.Type{name = "PMR_TYPE"}
                      @thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
                      1: string text;
                      @cpp.Type{name = "PMR_TYPE"}
                      2: binary bytes;
                      @cpp.Type{name = "PMR_TYPE"}
                      3: Text alias;
                    }
                    service Api {
                      void call(
                        @cpp.Type{name = "PMR_TYPE"} 1: Text text,
                        @cpp.Type{name = "PMR_TYPE"} 2: Bytes bytes,
                        @cpp.Type{name = "PMR_TYPE"} 3: string direct_text,
                        @cpp.Type{name = "PMR_TYPE"} 4: binary direct_bytes);
                    }
                    """
                ).replace("PMR_TYPE", name)
                write_file("foo.thrift", before)
                binary = pkg_resources.resource_filename(__name__, "codemod")
                run_binary(binary, "foo.thrift")
                self.assertEqual(read_file("foo.thrift"), before)
                run_binary(binary, "foo.thrift")
                self.assertEqual(read_file("foo.thrift"), before)

    def test_pmr_lookalikes_are_not_preserved(self):
        for name in (
            "std::pmr::wstring",
            "::std::pmr::string_view",
            "std::basic_string<char>",
            "std::pmr::basic_string<char>",
            "std::pmr::str ing",
            "std::p mr::string",
            "std::pmr: :string",
            "std: :pmr::string",
            "std::pmr::stringExtra",
            "PMRStringAlias",
        ):
            with self.subTest(name=name):
                before = (
                    'include "thrift/annotation/cpp.thrift"\n'
                    '@cpp.Type{name = "PMR_TYPE"}\n'
                    "typedef string Text\n"
                    '@cpp.Type{name = "PMR_TYPE"}\n'
                    "typedef binary Bytes\n"
                ).replace("PMR_TYPE", name)
                expected = before.replace(
                    '@cpp.Type{name = "' + name + '"}',
                    '@cpp.Adapter{name = "::apache::thrift::StringTypeAdapter<'
                    + name
                    + '>"}',
                )
                self.check_transform(before, expected)

    def check_transform(self, before, expected):
        write_file("foo.thrift", before)
        binary = pkg_resources.resource_filename(__name__, "codemod")
        run_binary(binary, "foo.thrift")
        actual = read_file("foo.thrift")
        self.assertEqual(
            "\n".join(line.rstrip() for line in actual.splitlines()),
            "\n".join(line.rstrip() for line in expected.splitlines()),
        )
        run_binary(binary, "foo.thrift")
        self.assertEqual(read_file("foo.thrift"), actual)
