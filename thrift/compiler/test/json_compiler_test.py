#!/usr/bin/env/ python3
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

import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest

import pkg_resources


thrift = pkg_resources.resource_filename(__name__, "thrift")


def read_file(path):
    with open(path, "r") as f:
        return f.read()


def write_file(path, content):
    with open(path, "w") as f:
        f.write(content)


def check_run_thrift(annotate, *args):
    gen_str = "json"
    if annotate:
        gen_str = "json:annotate"
    argsx = [
        thrift,
        "--gen",
        gen_str,
    ] + list(args)
    pipe = subprocess.PIPE
    return subprocess.check_call(argsx, stdout=pipe, stderr=pipe)


def gen(path, content, annotate=False):
    base, ext = os.path.splitext(path)
    if ext != ".thrift":
        raise Exception("bad path: {}".format(path))
    write_file(path, content)
    check_run_thrift(annotate, "foo.thrift")
    return json.loads(read_file(os.path.join("gen-json", base + ".json")))


class JsonCompilerTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        self.tmp = tmp
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.tmp)
        self.maxDiff = None

    def test_empty(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
            },
        )

    def test_one_struct_empty(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            struct DataHolder {
            }
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "structs": {
                    "DataHolder": {
                        "fields": {},
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 1,
                        "source_range": {
                            "begin": {
                                "line": 1,
                                "column": 1,
                            },
                            "end": {
                                "line": 2,
                                "column": 2,
                            },
                        },
                    },
                },
            },
        )

    def test_one_struct_one_field_dep_struct(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            struct DataItem {
            }
            struct DataHolder {
                1: DataItem item,
            }
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "structs": {
                    "DataItem": {
                        "fields": {},
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 1,
                        "source_range": {
                            "begin": {
                                "line": 1,
                                "column": 1,
                            },
                            "end": {
                                "line": 2,
                                "column": 2,
                            },
                        },
                    },
                    "DataHolder": {
                        "fields": {
                            "item": {
                                "required": True,
                                "type_enum": "STRUCT",
                                "spec_args": "DataItem",
                                "source_range": {
                                    "begin": {
                                        "line": 4,
                                        "column": 5,
                                    },
                                    "end": {
                                        "line": 4,
                                        "column": 22,
                                    },
                                },
                            },
                        },
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 3,
                        "source_range": {
                            "begin": {
                                "line": 3,
                                "column": 1,
                            },
                            "end": {
                                "line": 5,
                                "column": 2,
                            },
                        },
                    },
                },
            },
        )

    def test_one_struct_one_field_dep_coll_struct(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            struct DataItem {
            }
            struct DataHolder {
                1: map<string, DataItem> item,
            }
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "structs": {
                    "DataItem": {
                        "fields": {},
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 1,
                        "source_range": {
                            "begin": {
                                "line": 1,
                                "column": 1,
                            },
                            "end": {
                                "line": 2,
                                "column": 2,
                            },
                        },
                    },
                    "DataHolder": {
                        "fields": {
                            "item": {
                                "required": True,
                                "type_enum": "MAP",
                                "spec_args": {
                                    "key_type": {
                                        "type_enum": "STRING",
                                        "spec_args": None,
                                    },
                                    "val_type": {
                                        "type_enum": "STRUCT",
                                        "spec_args": "DataItem",
                                    },
                                },
                                "source_range": {
                                    "begin": {
                                        "line": 4,
                                        "column": 5,
                                    },
                                    "end": {
                                        "line": 4,
                                        "column": 35,
                                    },
                                },
                            },
                        },
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 3,
                        "source_range": {
                            "begin": {
                                "line": 3,
                                "column": 1,
                            },
                            "end": {
                                "line": 5,
                                "column": 2,
                            },
                        },
                    },
                },
            },
        )

    def test_one_struct_one_field_dep_struct_bug_out_of_order(self):
        # when a field appeared with an unknown type, it is listed as typedef
        # in spec_args, even if later that type was defined as struct
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            struct DataHolder {
                1: DataItem item,
            }
            struct DataItem {
            }
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "structs": {
                    "DataItem": {
                        "fields": {},
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 4,
                        "source_range": {
                            "begin": {
                                "line": 4,
                                "column": 1,
                            },
                            "end": {
                                "line": 5,
                                "column": 2,
                            },
                        },
                    },
                    "DataHolder": {
                        "fields": {
                            "item": {
                                "required": True,
                                "type_enum": "STRUCT",
                                "spec_args": "DataItem",
                                "source_range": {
                                    "begin": {
                                        "line": 2,
                                        "column": 5,
                                    },
                                    "end": {
                                        "line": 2,
                                        "column": 22,
                                    },
                                },
                            },
                        },
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 1,
                        "source_range": {
                            "begin": {
                                "line": 1,
                                "column": 1,
                            },
                            "end": {
                                "line": 3,
                                "column": 2,
                            },
                        },
                    },
                },
            },
        )

    def test_const_map_bool_key(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            const map<bool, i32> flags = {true: 1, false: 0};
        """
            ),
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "constants": {
                    "flags": {
                        "lineno": 1,
                        "value": {"true": 1, "false": 0},
                        "type_enum": "MAP",
                        "spec_args": {
                            "key_type": {"type_enum": "BOOL", "spec_args": None},
                            "val_type": {"type_enum": "I32", "spec_args": None},
                        },
                        "source_range": {
                            "begin": {
                                "line": 1,
                                "column": 1,
                            },
                            "end": {
                                "line": 1,
                                "column": 50,
                            },
                        },
                    },
                },
            },
        )

    def test_struct_field_annotate(self):
        obj = gen(
            "foo.thrift",
            textwrap.dedent(
                """\
            include "thrift/annotation/thrift.thrift"

            struct Annotation {
              1: string name;
            }

            @Annotation{name="aba"}
            @thrift.DeprecatedUnvalidatedAnnotations{
              items = {"my_annotation_key": "my_annotation_value"},
            }
            struct DataHolder {
              @thrift.DeprecatedUnvalidatedAnnotations{
                items = {"foo": "bar", "ignore": "1"},
              }
              1: string data
            }
        """
            ),
            annotate=True,
        )
        self.assertEqual(
            obj,
            {
                "__fbthrift": {"@" + "generated": 0},
                "thrift_module": "foo",
                "includes": {
                    "thrift": {
                        "path": "thrift/annotation/thrift.thrift",
                    },
                },
                "structs": {
                    "Annotation": {
                        "fields": {
                            "name": {
                                "required": True,
                                "spec_args": None,
                                "type_enum": "STRING",
                                "source_range": {
                                    "begin": {
                                        "line": 4,
                                        "column": 3,
                                    },
                                    "end": {
                                        "line": 4,
                                        "column": 18,
                                    },
                                },
                            },
                        },
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 3,
                        "source_range": {
                            "begin": {
                                "line": 3,
                                "column": 1,
                            },
                            "end": {
                                "line": 5,
                                "column": 2,
                            },
                        },
                    },
                    "DataHolder": {
                        "fields": {
                            "data": {
                                "required": True,
                                "type_enum": "STRING",
                                "spec_args": None,
                                "annotations": {
                                    "foo": {
                                        "value": "bar",
                                        "source_range": {
                                            "begin": {
                                                "line": 13,
                                                "column": 14,
                                            },
                                            "end": {
                                                "line": 13,
                                                "column": 26,
                                            },
                                        },
                                    },
                                    "ignore": {
                                        "value": "1",
                                        "source_range": {
                                            "begin": {
                                                "line": 13,
                                                "column": 28,
                                            },
                                            "end": {
                                                "line": 13,
                                                "column": 41,
                                            },
                                        },
                                    },
                                },
                                "structured_annotations": {
                                    "thrift.DeprecatedUnvalidatedAnnotations": {
                                        "items": {
                                            "foo": "bar",
                                            "ignore": "1",
                                        },
                                    },
                                },
                                "source_range": {
                                    "begin": {
                                        "line": 12,
                                        "column": 3,
                                    },
                                    "end": {
                                        "line": 15,
                                        "column": 17,
                                    },
                                },
                            },
                        },
                        "is_exception": False,
                        "is_union": False,
                        "lineno": 7,
                        "annotations": {
                            "my_annotation_key": {
                                "value": "my_annotation_value",
                                "source_range": {
                                    "begin": {
                                        "line": 9,
                                        "column": 12,
                                    },
                                    "end": {
                                        "line": 9,
                                        "column": 54,
                                    },
                                },
                            },
                        },
                        "structured_annotations": {
                            "Annotation": {
                                "name": "aba",
                            },
                            "thrift.DeprecatedUnvalidatedAnnotations": {
                                "items": {
                                    "my_annotation_key": "my_annotation_value",
                                },
                            },
                        },
                        "source_range": {
                            "begin": {
                                "line": 7,
                                "column": 1,
                            },
                            "end": {
                                "line": 16,
                                "column": 2,
                            },
                        },
                    },
                },
            },
        )
