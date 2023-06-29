/* Copyright (c) 2018-2022 Marcelo Zimbres Silva (mzimbres@gmail.com)
 *
 * Distributed under the Boost Software License, Version 1.0. (See
 * accompanying file LICENSE.txt)
 */

#include <boost/asio.hpp>
#if defined(BOOST_ASIO_HAS_CO_AWAIT)
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <aedis.hpp>
#include <map>
#include <vector>

#include "common/common.hpp"

namespace net = boost::asio;
namespace resp3 = aedis::resp3;
using namespace net::experimental::awaitable_operators;
using aedis::adapt;

void print(std::map<std::string, std::string> const& cont)
{
   for (auto const& e: cont)
      std::cout << e.first << ": " << e.second << "\n";
}

void print(std::vector<int> const& cont)
{
   for (auto const& e: cont) std::cout << e << " ";
   std::cout << "\n";
}

auto run(std::shared_ptr<connection> conn, std::string host, std::string port) -> net::awaitable<void>
{
   co_await connect(conn, host, port);
   co_await conn->async_run();
}

// Stores the content of some STL containers in Redis.
auto store(std::shared_ptr<connection> conn) -> net::awaitable<void>
{
   std::vector<int> vec
      {1, 2, 3, 4, 5, 6};

   std::map<std::string, std::string> map
      {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}};

   resp3::request req;
   req.push("HELLO", 3);
   req.push_range("RPUSH", "rpush-key", vec);
   req.push_range("HSET", "hset-key", map);

   co_await conn->async_exec(req);
}

auto hgetall(std::shared_ptr<connection> conn) -> net::awaitable<void>
{
   // A request contains multiple commands.
   resp3::request req;
   req.push("HELLO", 3);
   req.push("HGETALL", "hset-key");

   // Responses as tuple elements.
   std::tuple<aedis::ignore, std::map<std::string, std::string>> resp;

   // Executes the request and reads the response.
   co_await conn->async_exec(req, adapt(resp));

   print(std::get<1>(resp));
}

// Retrieves in a transaction.
auto transaction(std::shared_ptr<connection> conn) -> net::awaitable<void>
{
   resp3::request req;
   req.push("HELLO", 3);
   req.push("MULTI");
   req.push("LRANGE", "rpush-key", 0, -1); // Retrieves
   req.push("HGETALL", "hset-key"); // Retrieves
   req.push("EXEC");

   std::tuple<
      aedis::ignore, // hello
      aedis::ignore, // multi
      aedis::ignore, // lrange
      aedis::ignore, // hgetall
      std::tuple<std::optional<std::vector<int>>, std::optional<std::map<std::string, std::string>>> // exec
   > resp;

   co_await conn->async_exec(req, adapt(resp));

   print(std::get<0>(std::get<4>(resp)).value());
   print(std::get<1>(std::get<4>(resp)).value());
}

auto quit(std::shared_ptr<connection> conn) -> net::awaitable<void>
{
   resp3::request req;
   req.push("QUIT");

   co_await conn->async_exec(req);
}

// Called from the main function (see main.cpp)
net::awaitable<void> co_main(std::string host, std::string port)
{
   auto ex = co_await net::this_coro::executor;
   auto conn = std::make_shared<connection>(ex);
   net::co_spawn(ex, run(conn, host, port), net::detached);
   co_await store(conn);
   co_await transaction(conn);
   co_await hgetall(conn);
   co_await quit(conn);
}

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)
