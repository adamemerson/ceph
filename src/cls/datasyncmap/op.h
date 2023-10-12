// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file cls/datasyncmap/ops.hh
 *
 * Class/operation names for datasync map
 */

#pragma once

#include <string>

#include "include/buffer.h"
#include "include/encoding.h"

namespace rados::cls::datasyncmap::op {
inline constexpr auto CLASS = "datasyncmap";
inline constexpr auto INSERT = "insert";
inline constexpr auto ERASE = "erase";
inline constexpr auto LIST = "list";
}
