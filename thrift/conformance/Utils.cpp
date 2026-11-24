/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thrift/conformance/Utils.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <optional>

#include <fmt/ranges.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/Subprocess.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

namespace apache::thrift::conformance {

const char* getEnvOr(const char* name, const char* dflt) {
  if (const char* value = std::getenv(name)) {
    return value;
  }
  return dflt;
}

std::vector<std::string> javaLaunchArgs(const std::string& jar) {
  if (const char* javaBinary = std::getenv("THRIFT_CONFORMANCE_JAVA_BINARY")) {
    return {javaBinary, "-jar", jar};
  }
  return {"/usr/bin/env", "java", "-jar", jar};
}

const char* getEnvOrThrow(const char* name) {
  if (const char* value = std::getenv(name)) {
    return value;
  }
  folly::throw_exception<std::runtime_error>(
      fmt::format("{} environment variable is required.", name));
}

std::string readFromFile(const char* filename) {
  if (filename == nullptr) {
    return {};
  }
  std::string data;
  if (!folly::readFile(filename, data)) {
    folly::throw_exception<std::invalid_argument>(
        fmt::format("Could not read file: {}", filename));
  }
  return data;
}

std::string readFromCmd(const std::vector<std::string>& argv) {
  folly::Subprocess proc(argv, folly::Subprocess::Options().pipeStdout());
  std::string result = proc.communicate().first;
  // NOTE: proc.waitOrTermiateOrKill() logs, so we can't use it before main().
  proc.sendSignal(SIGINT);
  if (proc.waitTimeout(std::chrono::seconds(1)).running()) {
    proc.terminate();
    if (proc.waitTimeout(std::chrono::seconds(1)).running()) {
      proc.kill();
      proc.wait();
    }
  }

  return result;
}

std::pair<std::string_view, std::string_view> parseNameAndCmd(
    std::string_view entry) {
  // Look for a custom name.
  auto pos = entry.find_last_of("#/");
  if (pos != std::string_view::npos && entry[pos] == '#') {
    if (pos == entry.size() - 1) {
      // Just a trailing delim, remove it.
      entry = entry.substr(0, pos);
    } else {
      // Use the custom name.
      return {entry.substr(pos + 1), entry.substr(0, pos)};
    }
  }

  // No custom name, so use parent directory as name.
  size_t stop = entry.find_last_of("\\/") - 1;
  size_t start = entry.find_last_of("\\/", stop);
  return {entry.substr(start + 1, stop - start), entry};
}

std::map<std::string_view, std::string_view> parseCmds(
    std::string_view cmdsStr) {
  std::map<std::string_view, std::string_view> result;
  if (cmdsStr.empty()) {
    return result;
  }
  // category e.g. `rust[client=cpp,tls]` can hold commas
  std::vector<std::string_view> cmds;
  size_t depth = 0;
  size_t start = 0;
  for (size_t i = 0; i <= cmdsStr.size(); ++i) {
    if (i == cmdsStr.size() || (cmdsStr[i] == ',' && depth == 0)) {
      cmds.push_back(cmdsStr.substr(start, i - start));
      start = i + 1;
    } else if (cmdsStr[i] == '[') {
      ++depth;
    } else if (cmdsStr[i] == ']' && depth > 0) {
      --depth;
    }
  }
  for (auto cmd : cmds) {
    auto entry = parseNameAndCmd(folly::trimWhitespace(cmd));
    auto res = result.emplace(entry);
    if (!res.second) {
      folly::throw_exception<std::invalid_argument>(fmt::format(
          "Multiple servers have the name {}: {} vs {}",
          entry.first,
          res.first->second,
          entry.second));
    }
  }
  return result;
}

namespace {

// apply globs the same way `--gtest_filter` does. `*` matches many characters
// and `?` matches a single one.
bool globMatches(std::string_view glob, std::string_view text) {
  size_t g = 0;
  size_t t = 0;
  size_t star = std::string_view::npos;
  size_t resume = 0;
  while (t < text.size()) {
    if (g < glob.size() && (glob[g] == '?' || glob[g] == text[t])) {
      ++g;
      ++t;
    } else if (g < glob.size() && glob[g] == '*') {
      star = g++;
      resume = t;
    } else if (star != std::string_view::npos) {
      g = star + 1;
      t = ++resume;
    } else {
      return false;
    }
  }
  while (g < glob.size() && glob[g] == '*') {
    ++g;
  }
  return g == glob.size();
}

bool isGlob(std::string_view text) {
  return text.find_first_of("*?") != std::string_view::npos;
}

struct Name {
  std::string_view lang;
  std::optional<std::vector<std::string_view>> config;
  std::string_view rest;
};

// `lang/rest`, or `lang[config]/rest`
std::optional<Name> splitName(std::string_view name) {
  auto slash = name.find('/');
  if (slash == std::string_view::npos || slash + 1 == name.size()) {
    return std::nullopt;
  }
  Name result{name.substr(0, slash), std::nullopt, name.substr(slash + 1)};
  if (auto open = result.lang.find('['); open != std::string_view::npos) {
    if (!result.lang.ends_with(']')) {
      return std::nullopt;
    }
    folly::split(
        ',',
        result.lang.substr(open + 1, result.lang.size() - open - 2),
        result.config.emplace());
    result.lang = result.lang.substr(0, open);
  }
  if (result.lang.empty()) {
    return std::nullopt;
  }
  return result;
}

std::string canonicalName(const Name& name) {
  if (!name.config) {
    return fmt::format("{}/{}", name.lang, name.rest);
  }
  auto config = *name.config;
  std::ranges::sort(config);
  return fmt::format("{}[{}]/{}", name.lang, fmt::join(config, ","), name.rest);
}

bool configMatches(
    const std::optional<std::vector<std::string>>& listed,
    const std::optional<std::vector<std::string_view>>& config) {
  if (!listed) {
    return !config;
  }
  const auto attrs = config.value_or(std::vector<std::string_view>{});
  auto covers = [&](std::string_view attr) {
    return std::ranges::any_of(*listed, [&](const auto& glob) {
      return glob != "*" && globMatches(glob, attr);
    });
  };
  auto present = [&](const std::string& glob) {
    return glob == "*" || std::ranges::any_of(attrs, [&](auto attr) {
             return globMatches(glob, attr);
           });
  };
  auto open = std::ranges::any_of(
      *listed, [](const auto& glob) { return glob == "*"; });
  return std::ranges::all_of(*listed, present) &&
      (open || std::ranges::all_of(attrs, covers));
}

} // namespace

Nonconforming::Nonconforming(std::string_view data) {
  std::vector<std::string_view> lines;
  folly::split('\n', data, lines);
  for (size_t i = 0; i < lines.size(); ++i) {
    std::string_view text =
        folly::trimWhitespace(lines[i].substr(0, lines[i].find('#')));
    if (text.empty()) {
      continue;
    }
    auto name = splitName(text);
    const auto config = name
        ? name->config.value_or(std::vector<std::string_view>{})
        : std::vector<std::string_view>{};
    if (!name ||
        std::ranges::any_of(config, [](auto attr) { return attr.empty(); })) {
      folly::throw_exception<std::invalid_argument>(fmt::format(
          "nonconforming.txt:{}: expected `lang/...` or `lang[config]/...`, "
          "got {}",
          i + 1,
          text));
    }
    Entry entry{.line = i + 1, .text = std::string(text)};
    auto suite = name->rest.substr(0, name->rest.find('/'));
    if (!isGlob(name->lang) && !isGlob(suite) &&
        std::ranges::none_of(config, isGlob)) {
      scoped_.emplace_back(
          canonicalName(Name{name->lang, name->config, suite}), entry);
    }
    if (isGlob(name->lang) || isGlob(name->rest) ||
        std::ranges::any_of(config, isGlob)) {
      globs_.push_back(
          Glob{
              .entry = std::move(entry),
              .lang = std::string(name->lang),
              .config = name->config
                  ? std::optional(
                        std::vector<std::string>(config.begin(), config.end()))
                  : std::nullopt,
              .rest = std::string(name->rest),
          });
    } else {
      exact_.emplace(canonicalName(*name), std::move(entry));
    }
  }
}

std::optional<Nonconforming::Entry> Nonconforming::find(
    std::string_view test) const {
  auto name = splitName(test);
  if (!name) {
    return std::nullopt;
  }
  const Entry* found = nullptr;
  if (auto it = exact_.find(canonicalName(*name)); it != exact_.end()) {
    found = &it->second;
  }
  for (const auto& glob : globs_) {
    if (found && found->line < glob.entry.line) {
      break;
    }
    if (globMatches(glob.lang, name->lang) &&
        configMatches(glob.config, name->config) &&
        globMatches(glob.rest, name->rest)) {
      return glob.entry;
    }
  }
  return found ? std::optional(*found) : std::nullopt;
}

std::vector<Nonconforming::Entry> Nonconforming::stale(
    const std::set<std::string>& scope, const std::set<size_t>& used) const {
  std::set<std::string> canonical;
  for (const auto& key : scope) {
    if (auto name = splitName(key)) {
      canonical.insert(canonicalName(*name));
    }
  }
  std::vector<Entry> result;
  for (const auto& [key, entry] : scoped_) {
    if (canonical.contains(key) && !used.contains(entry.line)) {
      result.push_back(entry);
    }
  }
  return result;
}

namespace {

class StaleTest : public testing::Test {
 public:
  explicit StaleTest(std::vector<Nonconforming::Entry> stale)
      : stale_(std::move(stale)) {}

 protected:
  void TestBody() override {
    std::string listed;
    for (const auto& entry : stale_) {
      listed += fmt::format(
          "    thrift/conformance/data/nonconforming.txt:{}: {}\n",
          entry.line,
          entry.text);
    }
    EXPECT_TRUE(stale_.empty())
        << "These entries name none of this target's tests, or only tests an "
           "earlier entry names, though it runs their category and suite. "
           "Delete them:\n"
        << listed;
  }

 private:
  const std::vector<Nonconforming::Entry> stale_;
};

} // namespace

void registerStaleTest(
    std::vector<Nonconforming::Entry> stale, const char* file, int line) {
  registerTest(
      "Nonconforming",
      "NoStaleEntries",
      nullptr,
      nullptr,
      file,
      line,
      [stale = std::move(stale)]() { return new StaleTest(stale); });
}

std::string Nonconforming::Entry::explainConforming(
    std::string_view test) const {
  return fmt::format(
      "{} conforms, but is named by\n"
      "    thrift/conformance/data/nonconforming.txt:{}: {}\n"
      "If intentional, delete that line or narrow it so it no longer names "
      "this test (thrift/conformance/README.md shows how to list what it "
      "names).",
      test,
      line,
      text);
}

std::vector<TestSuite> getSuites() {
  std::vector<TestSuite> result;

  std::vector<std::string> suiteGens;
  folly::split(
      ',', getEnvOr("THRIFT_CONFORMANCE_TEST_SUITE_GENS", ""), suiteGens);
  for (auto cmd : suiteGens) {
    if (!cmd.empty()) {
      result.emplace_back(
          BinarySerializer::deserialize<TestSuite>(
              readFromCmd({std::move(cmd)})));
    }
  }
  if (result.empty()) {
    folly::throw_exception<std::invalid_argument>(fmt::format(
        "No test suites found:\n"
        "  THRIFT_CONFORMANCE_TEST_SUITE_GENS={}",
        getEnvOr("THRIFT_CONFORMANCE_TEST_SUITE_GENS", "<unset>")));
  }
  return result;
}

Nonconforming getNonconforming() {
  return Nonconforming(
      readFromFile(std::getenv("THRIFT_CONFORMANCE_NONCONFORMING")));
}

} // namespace apache::thrift::conformance
