/* Copyright (c) 2018-2022 Marcelo Zimbres Silva (mzimbres@gmail.com)
 *
 * Distributed under the Boost Software License, Version 1.0. (See
 * accompanying file LICENSE.txt)
 */

#ifndef AEDIS_RESP3_WRITE_HPP
#define AEDIS_RESP3_WRITE_HPP

#include <boost/asio/write.hpp>

namespace aedis::resp3 {

/** \brief Writes a request synchronously.
 *  \ingroup low-level-api
 *
 *  \param stream Stream to write the request to.
 *  \param req Request to write.
 */
template<
   class SyncWriteStream,
   class Request
   >
auto write(SyncWriteStream& stream, Request const& req)
{
   return boost::asio::write(stream, boost::asio::buffer(req.payload()));
}

template<
    class SyncWriteStream,
    class Request
    >
auto write(
    SyncWriteStream& stream,
    Request const& req,
    boost::system::error_code& ec)
{
   return boost::asio::write(stream, boost::asio::buffer(req.payload()), ec);
}

/** \brief Writes a request asynchronously.
 *  \ingroup low-level-api
 *
 *  \param stream Stream to write the request to.
 *  \param req Request to write.
 *  \param token Asio completion token.
 */
template<
   class AsyncWriteStream,
   class Request,
   class CompletionToken = boost::asio::default_completion_token_t<typename AsyncWriteStream::executor_type>
   >
auto async_write(
   AsyncWriteStream& stream,
   Request const& req,
   CompletionToken&& token =
      boost::asio::default_completion_token_t<typename AsyncWriteStream::executor_type>{})
{
   return boost::asio::async_write(stream, boost::asio::buffer(req.payload()), token);
}

} // aedis::resp3

#endif // AEDIS_RESP3_WRITE_HPP
