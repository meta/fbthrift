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

# pyre-strict
"""
Prints the expanded test cases listed in nonconforming.txt, one per-line, with
any globs or config options expanded. e.g.:

    buck2 run fbsource//xplat/thrift/conformance/data:nonconforming -- java2
    buck2 run fbsource//xplat/thrift/conformance/data:nonconforming -- --stale

This builds every conformance test target to list tests so it prints exactly
what the tests treat as nonconforming. `--stale` prints any entries that do not
name a test, e.g. they are garbage and can be removed.
"""

import argparse
import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

TARGETS = 'kind("cxx_test", fbcode//thrift/conformance/test/...)'
NONCONFORMING = "xplat/thrift/conformance/data/nonconforming.txt"
LOCATION = re.compile(r"\$\((?:location|exe) ([^)]+)\)")
CATEGORIES = (
    "THRIFT_CONFORMANCE_CLIENT_BINARIES",
    "THRIFT_CONFORMANCE_SERVER_BINARIES",
    "THRIFT_CONFORMANCE_SERVERS",
    "THRIFT_CONFORMANCE_SETUP_SERVERS",
)
GENS = "THRIFT_CONFORMANCE_TEST_SUITE_GENS"
ENTRY = re.compile(r"GetParam\(\) = nonconforming\.txt:(\d+)")

Json = dict[str, "Json"] | list["Json"] | str | int | float | bool | None


def default(value: Json) -> Json:
    while isinstance(value, dict) and value.get("__type") == "selector":
        entries = value["entries"]
        assert isinstance(entries, dict), f"unexpected select {value}"
        value = entries["DEFAULT"]
    return value


def env_of(attr: Json) -> dict[str, str]:
    attr = default(attr)
    assert isinstance(attr, dict), f"unexpected env {attr}"
    if attr.get("__type") == "concat":
        items = attr["items"]
        assert isinstance(items, list), f"unexpected concat {attr}"
        return {k: v for item in items for k, v in env_of(item).items()}
    return {k: str(default(v)) for k, v in attr.items()}


def categories(env: dict[str, str]) -> set[str]:
    return {
        entry.rsplit("#", 1)[1]
        for var in CATEGORIES
        for entry in re.split(r",(?![^\[]*\])", env.get(var, ""))
        if "#" in entry
    }


def lang(name: str) -> str:
    return re.split(r"[\[/]", name, maxsplit=1)[0]


def buck(*args: str) -> str:
    return subprocess.run(
        ["buck2", *args], check=True, stdout=subprocess.PIPE, text=True
    ).stdout


def list_tests(
    binary: str, env: dict[str, str], outputs: dict[str, str], nonconforming: Path
) -> list[tuple[str, int | None]]:
    """
    Return every test the binary registers with the line of the nonconforming entry.
    """
    run_env = (
        dict(os.environ)
        | {
            k: LOCATION.sub(lambda m: outputs[m.group(1)], v)
            if k == GENS
            else LOCATION.sub("/nonexistent", v)
            for k, v in env.items()
        }
        | {"THRIFT_CONFORMANCE_NONCONFORMING": str(nonconforming)}
    )
    listing = subprocess.run(
        [binary, "--gtest_list_tests"],
        env=run_env,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout
    tests, suite = [], ""
    for line in listing.splitlines():
        if not line.startswith(" "):
            suite = line.strip()
            continue
        name, _, comment = line.strip().partition("#")
        entry = ENTRY.search(comment)
        tests.append((suite + name.strip(), int(entry.group(1)) if entry else None))
    return tests


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lang", nargs="?", help="e.g. java2, rust, or cpp2")
    parser.add_argument(
        "--stale", action="store_true", help="print entries that name no test"
    )
    parser.add_argument(
        "--why", action="store_true", help="print the entry naming each test"
    )
    args = parser.parse_args()

    root = Path(buck("root", "--kind", "project").strip())
    nonconforming = root / NONCONFORMING
    targets = {
        target: env_of(attrs["env"])
        for target, attrs in json.loads(
            buck("uquery", TARGETS, "--output-attribute", "env", "--json")
        ).items()
    }
    targets = {
        target: env
        for target, env in targets.items()
        if args.lang is None or args.lang in {lang(c) for c in categories(env)}
    }
    gens = {g for env in targets.values() for g in LOCATION.findall(env.get(GENS, ""))}
    outputs = json.loads(
        buck("build", "--show-full-json-output", *sorted(targets.keys() | gens))
    )
    with ThreadPoolExecutor(max_workers=4) as pool:
        listings = pool.map(
            lambda t: list_tests(outputs[t], targets[t], outputs, nonconforming),
            sorted(targets),
        )
    named = {
        test: entry
        for listing in listings
        for test, entry in listing
        if entry is not None and (args.lang is None or lang(test) == args.lang)
    }

    if args.stale:
        used = set(named.values())
        for number, line in enumerate(nonconforming.read_text().splitlines(), 1):
            text = line.split("#", 1)[0].strip()
            if text and number not in used and args.lang in (None, lang(text)):
                print(f"nonconforming.txt:{number}: {text}")
        return
    for test in sorted(named):
        print(f"{test}  # nonconforming.txt:{named[test]}" if args.why else test)


if __name__ == "__main__":
    main()
