// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file cls/datasyncmap/cls.cc
 *
 * An OSD object class for managing the set of outstanding
 * bucketshards needing sync.
 */

#include <cstdint>
#include <limits>
#include <string>

#include "include/buffer.h"

#include "objclass/objclass.h"

#include "op.h"

using namespace std::literals;

CLS_VER(1,0)
CLS_NAME(datasyncmap)

using std::uint64_t;

namespace rados::cls::datasyncmap {
namespace {
static constexpr auto PREFIX = "_datasyncmap_"s;

/// Inserts an item into the set
///
/// \param key The key to insert into the set (A string)
///
/// \return 0 on success, error codes.
int insert(cls_method_context_t hctx, ceph::buffer::list* in,
           ceph::buffer::list* out)
{
  CLS_LOG(5, "%s", __PRETTY_FUNCTION__);

  std::string key;
  try {
    auto iter = in->cbegin();
    decode(key, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s: failed to decode request", __PRETTY_FUNCTION__);
    return -EINVAL;
  }

  if (key.empty()) {
    CLS_ERR("ERROR: %s: key may not be empty", __PRETTY_FUNCTION__);
    return -EINVAL;
  }

  const auto omapkey = PREFIX + key;
  // Originally I was planning to use a counter here, but since the
  // counter resets at zero on trim, that opens us to an A-B-A problem.
  uint64_t opaque = ceph::real_clock::now().time_since_epoch().count();
  ceph::buffer::list bl;
  encode(opaque, bl);
  auto r = cls_cxx_map_set_val(hctx, omapkey, &bl);
  if (r < 0) {
    CLS_ERR("%s: cls_cxx_map_set_val failed with key=%s, opaque=%" PRIu64 "r=%d",
            __PRETTY_FUNCTION__, key.c_str(), opaque, r);
  }

  return r;
}

/// Erases an item from the set
///
/// \param key The key to insert into the set (std::string)
/// \param ctr The opaque for the key (uin64_t)
///
/// \return 0 on success, error codes.
int erase(cls_method_context_t hctx, ceph::buffer::list* in,
	  ceph::buffer::list* out)
{
  CLS_LOG(5, "%s", __PRETTY_FUNCTION__);

  std::pair<std::string, uint64_t> datum;
  try {
    auto iter = in->cbegin();
    decode(datum, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s: failed to decode request", __PRETTY_FUNCTION__);
    return -EINVAL;
  }

  const auto& [key, inpaque] = datum;
  if (key.empty()) {
    CLS_ERR("ERROR: %s: key may not be empty", __PRETTY_FUNCTION__);
    return -EINVAL;
  }

  const auto omapkey = PREFIX + key;
  uint64_t opaque;
  ceph::buffer::list bl;
  {
    auto r = cls_cxx_map_get_val(hctx, omapkey, &bl);
    if (r == -ENOENT) {
      CLS_LOG(15, "%s: key=%s not found, returning success",
              __PRETTY_FUNCTION__, key.c_str());
      return 0;
    } else if (r == 0) {
      decode(opaque, bl);
    } else {
      CLS_ERR("%s: cls_cxx_map_get_val failed with key=%s, r=%d",
	      __PRETTY_FUNCTION__, key.c_str(), r);
      return r;
    }
  }

  if (opaque != inpaque) {
    CLS_LOG(20, "%s: key=%s has opaque=%" PRIu64 ", while inpaque=%" PRIu64,
	    __PRETTY_FUNCTION__, key.c_str(), opaque, inpaque);
    return -ECANCELED;
  }
  auto r = cls_cxx_map_remove_key(hctx, omapkey);
  if (r < 0) {
    CLS_ERR("%s: cls_cxx_map_remove_key failed with key=%s, r=%d",
            __PRETTY_FUNCTION__, key.c_str(), r);
  }

  return r;
}

/// Lists items in the set
///
/// \param count Number of items to return (uint64_t)
/// \param start Return items after this (std::string)
///
/// \return {(key, ctr)...} more
int list(cls_method_context_t hctx, ceph::buffer::list* in,
	 ceph::buffer::list* out)
{
  CLS_LOG(5, "%s", __PRETTY_FUNCTION__);
  std::pair<uint64_t, std::string> arg;
  try {
    auto iter = in->cbegin();
    decode(arg, iter);
  } catch (const ceph::buffer::error& err) {
    CLS_ERR("ERROR: %s: failed to decode request", __PRETTY_FUNCTION__);
    return -EINVAL;
  }
  const auto count = (arg.first == 0 ?
		      std::numeric_limits<uint64_t>::max() :
		      arg.first);
  const auto start = PREFIX + std::move(arg.second);

  std::map<std::string, ceph::buffer::list> vals;
  bool more;
  auto r = cls_cxx_map_get_vals(hctx, start, PREFIX, count,
				&vals, &more);
  if (r < 0) {
    CLS_ERR("%s: cls_cxx_map_get_vals failed with r=%d",
            __PRETTY_FUNCTION__, r);
    return r;
  }

  uint32_t c = vals.size();
  encode(c, *out);
  for (const auto& [omapkey, bl] : vals) {
    std::string_view key{omapkey};
    key.remove_prefix(PREFIX.size());
    uint64_t ctr;
    decode(ctr, bl);

    encode(key, *out);
    encode(ctr, *out);
  }
  encode(more, *out);

  return 0;
}
}
} // namespace rados::cls::datasyncmap

CLS_INIT(datasyncmap)
{
  using namespace rados::cls::datasyncmap;
  CLS_LOG(10, "Loaded datasyncmap class!");

  cls_handle_t h_class;
  cls_method_handle_t h_insert;
  cls_method_handle_t h_erase;
  cls_method_handle_t h_list;

  cls_register(op::CLASS, &h_class);
  cls_register_cxx_method(h_class, op::INSERT,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &insert, &h_insert);
  cls_register_cxx_method(h_class, op::ERASE,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &erase, &h_erase);
  cls_register_cxx_method(h_class, op::LIST,
                          CLS_METHOD_RD,
                          &list, &h_list);

  return;
}
