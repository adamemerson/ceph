// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include "neorados/cls/datasyncmap.h"

#include <coroutine>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

using namespace std::literals;

namespace asio = boost::asio;
namespace sys = boost::system;

namespace datasyncmap = neorados::cls::datasyncmap;

using neorados::ReadOp;
using neorados::WriteOp;

CORO_TEST_F(cls_datasyncmap, test_insert, NeoRadosTest)
{
  static const std::string key({0, 1, 2, 3, 0});
  std::string_view oid = "obj";
  co_await create_obj(oid);

  assert(std::ssize(key) == 5);

  // Check round-tripping
  co_await execute(oid, WriteOp{}.exec(datasyncmap::insert(key)));
  std::array<datasyncmap::entry, 1> estore;
  uint64_t oldpaque = 0;

  {
    auto [entries, more] = co_await datasyncmap::list(
      rados(), oid, pool(), {}, {estore}, asio::use_awaitable);
    EXPECT_EQ(1, std::size(entries));
    EXPECT_EQ(key, entries[0].key);
    EXPECT_FALSE(more);
    oldpaque = entries[0].opaque;
  }

  co_await execute(oid, WriteOp{}.exec(datasyncmap::insert(key)));
  auto [entries, more] = co_await datasyncmap::list(
    rados(), oid, pool(), {}, {estore}, asio::use_awaitable);
  EXPECT_EQ(1, std::size(entries));
  EXPECT_EQ(key, entries[0].key);
  EXPECT_FALSE(more);
  EXPECT_NE(entries[0].opaque, oldpaque);

  co_return;
}

CORO_TEST_F(cls_datasyncmap, test_erase, NeoRadosTest) {
  static const std::string key({0, 1, 2, 3, 0});
  std::string_view oid = "obj";
  co_await create_obj(oid);

  assert(std::ssize(key) == 5);

  // Check round-tripping
  co_await execute(oid, WriteOp{}.exec(datasyncmap::insert(key)));
  std::array<datasyncmap::entry, 1> estore;

  uint64_t opaque = 0;
  {
    auto [entries, more] = co_await datasyncmap::list(
      rados(), oid, pool(), {}, {estore}, asio::use_awaitable);
    EXPECT_EQ(1, std::size(entries));
    EXPECT_EQ(key, entries[0].key);
    EXPECT_FALSE(more);
    opaque = entries[0].opaque;
  }
  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(datasyncmap::erase(key, opaque - 1))),
    sys::errc::operation_canceled);
  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(datasyncmap::erase(key, opaque + 1))),
    sys::errc::operation_canceled);
  co_await execute(oid, WriteOp{}.exec(datasyncmap::erase(key, opaque)));
  auto [entries, more] = co_await datasyncmap::list(
    rados(), oid, pool(), {}, {estore}, asio::use_awaitable);
  EXPECT_TRUE(entries.empty());
}

CORO_TEST_F(cls_datasyncmap, test_list, NeoRadosTest) {
  std::string_view oid = "obj";
  co_await create_obj(oid);

  std::unordered_set<std::string> keys;
  for (char c = 0; c < 9; ++c) {
    keys.insert(std::string({c}));
    co_await execute(oid, WriteOp{}.exec(
		       datasyncmap::insert(std::string({c}))));
  }
  auto more = true;
  std::string cursor;
  std::array<datasyncmap::entry, 3> estore;
  std::unordered_set<std::string> got_keys;
  while (more) {
    auto [entries, more] = co_await datasyncmap::list(
      rados(), oid, pool(), cursor, {estore}, asio::use_awaitable);
    if (entries.empty()) {
      break;
    }
    EXPECT_EQ(3, std::size(entries));
    EXPECT_GT(entries.front().key, cursor);
    for (const auto& [key, opaque] : entries) {
      auto [i, inserted] = got_keys.insert(key);
      EXPECT_TRUE(inserted);
    }
    cursor = entries.back().key;
  }
  EXPECT_EQ(keys, got_keys);

  co_return;
}
