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

#pragma once

/// \file neorados/cls/datasyncmap.h
///
/// \brief NeoRADOS interface to datasyncmap class
///

#include <cerrno>
#include <coroutine>
#include <span>
#include <string>
#include <utility>

#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/neorados/RADOS.hpp"

#include <include/buffer.h>
#include <include/encoding.h>

#include "cls/datasyncmap/op.h"

#include "neorados/cls/common.h"

namespace neorados::cls::datasyncmap {
/// \brief Insert an entry
///
/// Append a call to a write operation to insert an entry
///
/// \param key Entry to insert
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] auto insert(std::string_view key)
{
  namespace op = rados::cls::datasyncmap::op;
  buffer::list in;
  encode(key, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& wop) {
    wop.exec(op::CLASS, op::INSERT, in);
  }};
}

/// \brief Erase an entry
///
/// Append a call to a write operation to erase an entry. If the
/// opaque does not match, fails with ECANCELED.
///
/// \param key Entry to insert
/// \param opaque Value that must match for the erase to succeed
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] auto erase(std::string_view key, std::uint64_t opaque)
{
  namespace op = rados::cls::datasyncmap::op;
  buffer::list in;
  encode(key, in);
  encode(opaque, in);
  return ClsWriteOp{[in = std::move(in)](WriteOp& wop) {
    wop.exec(op::CLASS, op::ERASE, in);
  }};
}

struct entry {
  std::string key;
  std::uint64_t opaque = 0;
  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(key, bl);
    encode(opaque, bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(key, bl);
    decode(opaque, bl);
  }
};
WRITE_CLASS_ENCODER(entry)

/// \brief List entries in set
///
/// Append a call to a read operation that lists set entries
///
/// \param cursor Key from which to start listings, empty for beginning
/// \param out Span to hold output
/// \param more If there are more entries after this one
///
/// \return (Span of returned entries, more)
template<boost::asio::completion_token_for<
	   void(boost::system::error_code ec,
		std::span<entry> out, bool more)> CompletionToken,
	 std::size_t N = std::dynamic_extent>
auto list(RADOS& r, Object oid, IOContext ioc, std::string_view cursor,
	  std::span<entry, N> store, CompletionToken&& token) {
  namespace sys = boost::system;
  namespace asio = boost::asio;
  namespace op = rados::cls::datasyncmap::op;
  using ceph::encode;
  using ceph::decode;
  using sig = void(sys::error_code, std::span<entry>, bool);

  using asio::experimental::co_composed;

  buffer::list in;
  uint64_t sz = store.size();
  encode(sz, in);
  encode(cursor, in);
  return asio::async_initiate<CompletionToken, sig>
    (co_composed<sig>
     ([](auto state, RADOS& r, Object oid, IOContext ioc,
	 std::span<entry, N> store, buffer::list in) -> void {
       try {
	 ReadOp op;
	 buffer::list bl;
	 sys::error_code ec;
	 op.exec(op::CLASS, op::LIST, std::move(in), &bl, &ec);
	 co_await r.execute(std::move(oid), std::move(ioc), std::move(op),
			    nullptr, asio::deferred);
	 if (ec) {
	   co_return std::make_tuple(ec, std::span<entry>{}, false);
	 }
	 auto iter = bl.cbegin();
	 std::uint32_t sz;
	 decode(sz, iter);
	 auto more = false;
	 if (sz > store.size()) [[unlikely]] {
	   more = true;
	 }
	 std::span<entry> out(store.data(),
			      std::min(store.size(), std::size_t(sz)));
	 for (auto& e : out) {
	   decode(e, iter);
	 }
	 if (!more) [[likely]] {
	   decode(more, iter);
	 }
	 co_return std::make_tuple(sys::error_code{}, out, more);
       } catch (const sys::system_error& e) {
	 co_return std::make_tuple(e.code(), std::span<entry>{}, false);
       }
     }, r.get_executor()),
     token, std::ref(r), std::move(oid), std::move(ioc), store, std::move(in));
}
} // namespace neorados::cls::version
