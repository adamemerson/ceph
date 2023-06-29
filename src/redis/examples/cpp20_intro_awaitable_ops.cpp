/* Copyright (c) 2018-2022 Marcelo Zimbres Silva (mzimbres@gmail.com)
 *
 * Distributed under the Boost Software License, Version 1.0. (See
 * accompanying file LICENSE.txt)
 */

#include <boost/asio.hpp>
#if defined(BOOST_ASIO_HAS_CO_AWAIT)
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <aedis.hpp>
#include "common/common.hpp"

namespace net = boost::asio;
namespace resp3 = aedis::resp3;
using namespace net::experimental::awaitable_operators;
using aedis::adapt;

// Called from the main function (see main.cpp)
auto co_main(std::string host, std::string port) -> net::awaitable<void>
{
   resp3::request req;
   req.push("HELLO", 3);
   req.push("PING", "Hello world");
   req.push("QUIT");

   std::tuple<aedis::ignore, std::string, aedis::ignore> resp;

   auto conn = std::make_shared<connection>(co_await net::this_coro::executor);
   co_await connect(conn, host, port);
   co_await (conn->async_run() || conn->async_exec(req, adapt(resp)));

   std::cout << "PING: " << std::get<1>(resp) << std::endl;
}

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)
