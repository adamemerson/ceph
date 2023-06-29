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
using steady_timer = net::use_awaitable_t<>::as_default_on_t<net::steady_timer>;
using aedis::adapt;

/* This example will subscribe and read pushes indefinitely.
 *
 * To test send messages with redis-cli
 *
 *    $ redis-cli -3
 *    127.0.0.1:6379> PUBLISH channel some-message
 *    (integer) 3
 *    127.0.0.1:6379>
 *
 * To test reconnection try, for example, to close all clients currently
 * connected to the Redis instance
 *
 * $ redis-cli
 * > CLIENT kill TYPE pubsub
 */

// Receives pushes.
auto receiver(std::shared_ptr<connection> conn) -> net::awaitable<void>
{
   using resp_type = std::vector<resp3::node<std::string>>;
   for (resp_type resp;;) {
      co_await conn->async_receive(adapt(resp));
      std::cout << resp.at(1).value << " " << resp.at(2).value << " " << resp.at(3).value << std::endl;
      resp.clear();
   }
}

auto co_main(std::string host, std::string port) -> net::awaitable<void>
{
   auto ex = co_await net::this_coro::executor;
   auto conn = std::make_shared<connection>(ex);
   steady_timer timer{ex};

   resp3::request req;
   req.push("HELLO", 3);
   req.push("SUBSCRIBE", "channel");

   // The loop will reconnect on connection lost. To exit type Ctrl-C twice.
   for (;;) {
      co_await connect(conn, host, port);
      co_await ((conn->async_run() || healthy_checker(conn) || receiver(conn)) && conn->async_exec(req));

      conn->reset_stream();
      timer.expires_after(std::chrono::seconds{1});
      co_await timer.async_wait();
   }
}

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)
