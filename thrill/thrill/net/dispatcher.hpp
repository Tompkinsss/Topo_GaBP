/*******************************************************************************
 * thrill/net/dispatcher.hpp
 *
 * Asynchronous callback wrapper around select(), epoll(), or other kernel-level
 * dispatchers.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_DISPATCHER_HEADER
#define THRILL_NET_DISPATCHER_HEADER

#include <thrill/data/block.hpp>
#include <thrill/data/byte_block.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/connection.hpp>
#include <tlx/delegate.hpp>
#include <tlx/die.hpp>

#include <atomic>
#include <chrono>
#include <ctime>
#include <deque>
#include <functional>
#include <queue>
#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

//! Signature of timer callbacks.
using TimerCallback = tlx::delegate<bool (), mem::GPoolAllocator<char> >;

//! Signature of async connection readability/writability callbacks.
using AsyncCallback = tlx::delegate<bool (), mem::GPoolAllocator<char> >;

//! Signature of async read Buffer callbacks.
using AsyncReadBufferCallback = tlx::delegate<
    void (Connection& c, Buffer&& buffer), mem::GPoolAllocator<char> >;

//! Signature of async read ByteBlock callbacks.
using AsyncReadByteBlockCallback = tlx::delegate<
    void (Connection& c, data::PinnedByteBlockPtr&& bytes),
    mem::GPoolAllocator<char> >;

//! Signature of async write callbacks.
using AsyncWriteCallback = tlx::delegate<
    void (Connection&), mem::GPoolAllocator<char> >;

/******************************************************************************/

static constexpr bool debug_async = false;
static constexpr bool debug_async_send = false;
static constexpr bool debug_async_recv = false;

class AsyncReadBuffer
{
public:
    //! Construct buffered reader with callback
    AsyncReadBuffer(Connection& conn, size_t buffer_size,
                    const AsyncReadBufferCallback& callback)
        : conn_(&conn),
          buffer_(buffer_size),
          callback_(callback) {
        LOGC(debug_async)
            << "AsyncReadBuffer()"
            << " buffer_.size()=" << buffer_.size();
        conn_->rx_active_++;
    }

    //! non-copyable: delete copy-constructor
    AsyncReadBuffer(const AsyncReadBuffer&) = delete;
    //! non-copyable: delete assignment operator
    AsyncReadBuffer& operator = (const AsyncReadBuffer&) = delete;
    //! move-constructor: default
    AsyncReadBuffer(AsyncReadBuffer&&) = default;
    //! move-assignment operator: default
    AsyncReadBuffer& operator = (AsyncReadBuffer&&) = default;

    ~AsyncReadBuffer() {
        LOGC(debug_async)
            << "~AsyncReadBuffer()"
            << " buffer_.size()=" << buffer_.size();
    }

    //! Should be called when the socket is readable
    bool operator () () {
        LOGC(debug_async_recv)
            << "AsyncReadBuffer() recv"
            << " offset=" << read_size_
            << " size=" << buffer_.size() - read_size_;

        ssize_t r = conn_->RecvOne(
            buffer_.data() + read_size_, buffer_.size() - read_size_);

        if (r <= 0) {
            // these errors are acceptable: just redo the recv later.
            if (errno == EINTR || errno == EAGAIN) return true;

            // signal artificial IsDone, for clean up.
            read_size_ = buffer_.size();

            // these errors are end-of-file indications (both good and bad)
            if (errno == 0 || errno == EPIPE || errno == ECONNRESET) {
                if (callback_) callback_(*conn_, Buffer());
                return false;
            }
            throw Exception("AsyncReadBuffer() error in recv() on "
                            "connection " + conn_->ToString(), errno);
        }

        read_size_ += r;

        if (read_size_ == buffer_.size()) {
            DoCallback();
            conn_->rx_active_--;
            return false;
        }
        else {
            return true;
        }
    }

    bool IsDone() const { return read_size_ == buffer_.size(); }

    //! reference to Buffer
    Buffer& buffer() { return buffer_; }

    void DoCallback() {
        if (callback_) {
            callback_(*conn_, std::move(buffer_));
            callback_ = AsyncReadBufferCallback();
        }
    }

    void DoCallback(size_t size_check) {
        die_unequal(size_check, buffer_.size());
        return DoCallback();
    }

    //! Returns conn_
    Connection * connection() const { return conn_; }

    //! underlying buffer pointer
    uint8_t * data() { return buffer_.data(); }

    //! underlying buffer pointer
    const uint8_t * data() const { return buffer_.data(); }

    //! underlying buffer size
    size_t size() const { return buffer_.size(); }

private:
    //! Connection reference
    Connection* conn_;

    //! Receive buffer (allocates memory)
    Buffer buffer_;

    //! total size currently read
    size_t read_size_ = 0;

    //! functional object to call once data is complete
    AsyncReadBufferCallback callback_;
};

/******************************************************************************/

class AsyncWriteBuffer
{
public:
    //! Construct buffered writer with callback
    AsyncWriteBuffer(Connection& conn, Buffer&& buffer,
                     const AsyncWriteCallback& callback)
        : conn_(&conn),
          buffer_(std::move(buffer)),
          callback_(callback) {
        LOGC(debug_async)
            << "AsyncWriteBuffer()"
            << " buffer_.size()=" << buffer_.size();
        conn_->tx_active_++;
    }

    //! non-copyable: delete copy-constructor
    AsyncWriteBuffer(const AsyncWriteBuffer&) = delete;
    //! non-copyable: delete assignment operator
    AsyncWriteBuffer& operator = (const AsyncWriteBuffer&) = delete;
    //! move-constructor: default
    AsyncWriteBuffer(AsyncWriteBuffer&&) = default;
    //! move-assignment operator: default
    AsyncWriteBuffer& operator = (AsyncWriteBuffer&&) = default;

    ~AsyncWriteBuffer() {
        LOGC(debug_async)
            << "~AsyncWriteBuffer()"
            << " buffer_.size()=" << buffer_.size();
    }

    //! Should be called when the socket is writable
    bool operator () () {
        LOGC(debug_async_recv)
            << "AsyncWriteBuffer() send"
            << " offset=" << write_size_
            << " size=" << buffer_.size() - write_size_;

        ssize_t r = conn_->SendOne(
            buffer_.data() + write_size_, buffer_.size() - write_size_);

        if (r <= 0) {
            if (errno == EINTR || errno == EAGAIN) return true;

            // signal artificial IsDone, for clean up.
            write_size_ = buffer_.size();

            if (errno == EPIPE) {
                LOG1 << "AsyncWriteBuffer() got EPIPE";
                DoCallback();
                return false;
            }
            throw Exception("AsyncWriteBuffer() error in send", errno);
        }

        write_size_ += r;

        if (write_size_ == buffer_.size()) {
            DoCallback();
            conn_->tx_active_--;
            return false;
        }
        else {
            return true;
        }
    }

    bool IsDone() const { return write_size_ == buffer_.size(); }

    void DoCallback() {
        if (callback_) {
            callback_(*conn_);
            callback_ = AsyncWriteCallback();
        }
    }

    //! Returns conn_
    Connection * connection() const { return conn_; }

    //! underlying buffer pointer
    const uint8_t * data() const { return buffer_.data(); }

    //! underlying buffer size
    size_t size() const { return buffer_.size(); }

private:
    //! Connection reference
    Connection* conn_;

    //! Send buffer (owned by this writer)
    Buffer buffer_;

    //! total size currently written
    size_t write_size_ = 0;

    //! functional object to call once data is complete
    AsyncWriteCallback callback_;
};

/******************************************************************************/

class AsyncReadByteBlock
{
public:
    //! Construct block reader with callback
    AsyncReadByteBlock(Connection& conn, size_t size,
                       data::PinnedByteBlockPtr&& block,
                       const AsyncReadByteBlockCallback& callback)
        : conn_(&conn),
          block_(std::move(block)),
          size_(size),
          callback_(callback) {
        LOGC(debug_async)
            << "AsyncReadByteBlock()"
            << " block_=" << block_
            << " size_=" << size_;
        conn_->rx_active_++;
    }

    //! non-copyable: delete copy-constructor
    AsyncReadByteBlock(const AsyncReadByteBlock&) = delete;
    //! non-copyable: delete assignment operator
    AsyncReadByteBlock& operator = (const AsyncReadByteBlock&) = delete;
    //! move-constructor: default
    AsyncReadByteBlock(AsyncReadByteBlock&&) = default;
    //! move-assignment operator: default
    AsyncReadByteBlock& operator = (AsyncReadByteBlock&&) = default;

    ~AsyncReadByteBlock() {
        LOGC(debug_async)
            << "~AsyncReadByteBlock()"
            << " block_=" << block_
            << " size_=" << size_;
    }

    //! Should be called when the socket is readable
    bool operator () () {
        LOGC(debug_async_recv)
            << "AsyncReadByteBlock() recv"
            << " offset=" << pos_
            << " size=" << size_ - pos_;

        ssize_t r = conn_->RecvOne(
            block_->data() + pos_, size_ - pos_);

        if (r <= 0) {
            // these errors are acceptable: just redo the recv later.
            if (errno == EINTR || errno == EAGAIN) return true;

            // signal artificial IsDone, for clean up.
            pos_ = size_;

            // these errors are end-of-file indications (both good and bad)
            if (errno == 0 || errno == EPIPE || errno == ECONNRESET) {
                DoCallback();
                return false;
            }
            throw Exception("AsyncReadBlock() error in recv", errno);
        }

        pos_ += r;

        if (pos_ == size_) {
            DoCallback();
            conn_->rx_active_--;
            return false;
        }
        else {
            return true;
        }
    }

    bool IsDone() const {
        // done if block is already delivered to callback or size matches
        return !block_ || pos_ == size_;
    }

    data::PinnedByteBlockPtr& byte_block() { return block_; }

    void DoCallback() {
        if (callback_) {
            callback_(*conn_, std::move(block_));
            callback_ = AsyncReadByteBlockCallback();
        }
    }

    void DoCallback(size_t size_check) {
        die_unequal(size_check, size_);
        return DoCallback();
    }

    //! Returns conn_
    Connection * connection() const { return conn_; }

    //! underlying buffer pointer
    uint8_t * data() { return block_->data(); }

    //! underlying buffer pointer
    const uint8_t * data() const { return block_->data(); }

    //! underlying buffer size
    size_t size() const { return size_; }

private:
    //! Connection reference
    Connection* conn_;

    //! Receive block, holds a pin on the memory.
    data::PinnedByteBlockPtr block_;

    //! size currently read
    size_t pos_ = 0;

    //! total size to read
    size_t size_;

    //! functional object to call once data is complete
    AsyncReadByteBlockCallback callback_;
};

/******************************************************************************/

class AsyncWriteBlock
{
public:
    //! Construct block writer with callback
    AsyncWriteBlock(Connection& conn, data::PinnedBlock&& block,
                    const AsyncWriteCallback& callback)
        : conn_(&conn),
          block_(std::move(block)),
          callback_(callback) {
        LOGC(debug_async)
            << "AsyncWriteBlock()"
            << " block_.size()=" << block_.size()
            << " block_=" << block_;
        conn_->tx_active_++;
    }

    //! non-copyable: delete copy-constructor
    AsyncWriteBlock(const AsyncWriteBlock&) = delete;
    //! non-copyable: delete assignment operator
    AsyncWriteBlock& operator = (const AsyncWriteBlock&) = delete;
    //! move-constructor: default
    AsyncWriteBlock(AsyncWriteBlock&&) = default;
    //! move-assignment operator: default
    AsyncWriteBlock& operator = (AsyncWriteBlock&&) = default;

    ~AsyncWriteBlock() {
        LOGC(debug_async)
            << "~AsyncWriteBlock()"
            << " block_=" << block_;
    }

    //! Should be called when the socket is writable
    bool operator () () {
        LOGC(debug_async_send)
            << "AsyncWriteBlock() send"
            << " offset=" << written_size_
            << " size=" << block_.size() - written_size_;

        ssize_t r = conn_->SendOne(
            block_.data_begin() + written_size_,
            block_.size() - written_size_);

        if (r <= 0) {
            if (errno == EINTR || errno == EAGAIN) return true;

            // signal artificial IsDone, for clean up.
            written_size_ = block_.size();

            if (errno == EPIPE) {
                LOG1 << "AsyncWriteBlock() got EPIPE";
                DoCallback();
                return false;
            }
            throw Exception("AsyncWriteBlock() error in send", errno);
        }

        written_size_ += r;

        if (written_size_ == block_.size()) {
            DoCallback();
            conn_->tx_active_--;
            return false;
        }
        else {
            return true;
        }
    }

    bool IsDone() const { return written_size_ == block_.size(); }

    void DoCallback() {
        if (callback_) {
            callback_(*conn_);
            callback_ = AsyncWriteCallback();
        }
        // release Pin
        block_.Reset();
    }

    //! Returns conn_
    Connection * connection() const { return conn_; }

    //! underlying buffer pointer
    const uint8_t * data() const { return block_.data_begin(); }

    //! underlying buffer size
    size_t size() const { return block_.size(); }

private:
    //! Connection reference
    Connection* conn_;

    //! Send block (holds a pin on the underlying ByteBlock)
    data::PinnedBlock block_;

    //! total size currently written
    size_t written_size_ = 0;

    //! functional object to call once data is complete
    AsyncWriteCallback callback_;
};

/******************************************************************************/

/*!
 * Dispatcher is a high level wrapper for asynchronous callback processing.. One
 * can register Connection objects for readability and writability checks,
 * buffered reads and writes with completion callbacks, and also timer
 * functions.
 */
class Dispatcher
{
    static constexpr bool debug = false;

private:
    //! import into class namespace
    using steady_clock = std::chrono::steady_clock;

    //! import into class namespace
    using milliseconds = std::chrono::milliseconds;

    //! for access to terminate_
    friend class DispatcherThread;

public:
    //! default constructor
    Dispatcher() = default;

    //! non-copyable: delete copy-constructor
    Dispatcher(const Dispatcher&) = delete;
    //! non-copyable: delete assignment operator
    Dispatcher& operator = (const Dispatcher&) = delete;

    //! virtual destructor
    virtual ~Dispatcher() { }

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    void AddTimer(const std::chrono::milliseconds& timeout,
                  const TimerCallback& cb) {
        timer_pq_.emplace(steady_clock::now() + timeout,
                          std::chrono::duration_cast<milliseconds>(timeout),
                          cb);
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    virtual void AddRead(Connection& c, const AsyncCallback& read_cb) = 0;

    //! Register a buffered write callback and a default exception callback.
    virtual void AddWrite(Connection& c, const AsyncCallback& write_cb) = 0;

    //! Cancel all callbacks on a given connection.
    virtual void Cancel(Connection& c) = 0;

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    virtual void AsyncRead(Connection& c, uint32_t /* seq */, size_t size,
                           const AsyncReadBufferCallback& done_cb) {
        assert(c.IsValid());

        LOG << "async read on read dispatcher";
        if (size == 0) {
            if (done_cb) done_cb(c, Buffer());
            return;
        }

        // add new async reader object
        async_read_.emplace_back(c, size, done_cb);

        // register read callback
        AsyncReadBuffer& arb = async_read_.back();
        AddRead(c, AsyncCallback::make<
                    AsyncReadBuffer, &AsyncReadBuffer::operator ()>(&arb));
    }

    //! asynchronously read the full ByteBlock and deliver it to the callback
    virtual void AsyncRead(Connection& c, uint32_t /* seq */, size_t size,
                           data::PinnedByteBlockPtr&& block,
                           const AsyncReadByteBlockCallback& done_cb) {
        assert(c.IsValid());

        LOG << "async read on read dispatcher";
        if (block->size() == 0) {
            if (done_cb) done_cb(c, std::move(block));
            return;
        }

        // add new async reader object
        async_read_block_.emplace_back(c, size, std::move(block), done_cb);

        // register read callback
        AsyncReadByteBlock& arbb = async_read_block_.back();
        AddRead(c, AsyncCallback::make<
                    AsyncReadByteBlock, &AsyncReadByteBlock::operator ()>(&arbb));
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    virtual void AsyncWrite(
        Connection& c, uint32_t /* seq */, Buffer&& buffer,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) {
        assert(c.IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async writer object
        async_write_.emplace_back(c, std::move(buffer), done_cb);

        // register write callback
        AsyncWriteBuffer& awb = async_write_.back();
        AddWrite(c, AsyncCallback::make<
                     AsyncWriteBuffer, &AsyncWriteBuffer::operator ()>(&awb));
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    virtual void AsyncWrite(
        Connection& c, uint32_t /* seq */, data::PinnedBlock&& block,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) {
        assert(c.IsValid());

        if (block.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async writer object
        async_write_block_.emplace_back(c, std::move(block), done_cb);

        // register write callback
        AsyncWriteBlock& awb = async_write_block_.back();
        AddWrite(c, AsyncCallback::make<
                     AsyncWriteBlock, &AsyncWriteBlock::operator ()>(&awb));
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(
        Connection& c, uint32_t seq, const void* buffer, size_t size,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) {
        return AsyncWrite(c, seq, Buffer(buffer, size), done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(
        Connection& c, uint32_t seq, const std::string& str,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) {
        return AsyncWriteCopy(c, seq, str.data(), str.size(), done_cb);
    }

    //! \}

    //! \name Dispatch
    //! \{

    //! Dispatch one or more events
    void Dispatch() {
        // process timer events that lie in the past
        steady_clock::time_point now = steady_clock::now();

        while (!terminate_ &&
               !timer_pq_.empty() &&
               timer_pq_.top().next_timeout <= now)
        {
            const Timer& top = timer_pq_.top();
            if (top.cb()) {
                // requeue timeout event again.
                timer_pq_.emplace(top.next_timeout + top.timeout,
                                  top.timeout, top.cb);
            }
            timer_pq_.pop();
        }

        if (terminate_) return;

        // calculate time until next timer event
        if (timer_pq_.empty()) {
            LOG << "Dispatch(): empty timer queue - selecting for 10s";
            DispatchOne(milliseconds(10000));
        }
        else {
            auto diff = std::chrono::duration_cast<milliseconds>(
                timer_pq_.top().next_timeout - now);

            if (diff < milliseconds(1)) diff = milliseconds(1);

            sLOG << "Dispatch(): waiting" << diff.count() << "ms";
            DispatchOne(diff);
        }

        // clean up finished AsyncRead/Writes
        while (async_read_.size() && async_read_.front().IsDone()) {
            async_read_.pop_front();
        }
        while (async_write_.size() && async_write_.front().IsDone()) {
            async_write_.pop_front();
        }

        while (async_read_block_.size() && async_read_block_.front().IsDone()) {
            async_read_block_.pop_front();
        }
        while (async_write_block_.size() && async_write_block_.front().IsDone()) {
            async_write_block_.pop_front();
        }
    }

    //! Loop over Dispatch() until terminate_ flag is set.
    void Loop() {
        while (!terminate_) {
            Dispatch();
        }
    }

    //! Interrupt current dispatch
    virtual void Interrupt() = 0;

    //! Causes the dispatcher to break out after the next timeout occurred
    //! Does not interrupt the currently running read/write operation, but
    //! breaks after the operation finished or timed out.
    void Terminate() {
        terminate_ = true;
    }

    //! Check whether there are still AsyncWrite()s in the queue.
    bool HasAsyncWrites() const {
        return (async_write_.size() != 0) || (async_write_block_.size() != 0);
    }

    //! \}

protected:
    virtual void DispatchOne(const std::chrono::milliseconds& timeout) = 0;

    //! Default exception handler
    static bool ExceptionCallback(Connection& c) {
        // exception on listen socket ?
        throw Exception(
                  "Dispatcher() exception on socket fd "
                  + c.ToString() + "!", errno);
    }

    //! true if dispatcher needs to stop
    std::atomic<bool> terminate_ { false };

    /*------------------------------------------------------------------------*/

    //! struct for timer callbacks
    struct Timer {
        //! timepoint of next timeout
        steady_clock::time_point next_timeout;
        //! relative timeout for restarting
        milliseconds             timeout;
        //! callback
        TimerCallback            cb;

        Timer(const steady_clock::time_point& _next_timeout,
              const milliseconds& _timeout,
              const TimerCallback& _cb)
            : next_timeout(_next_timeout),
              timeout(_timeout),
              cb(_cb)
        { }

        bool operator < (const Timer& b) const
        { return next_timeout > b.next_timeout; }
    };

    //! priority queue of timer callbacks
    using TimerPQ = std::priority_queue<
        Timer, std::vector<Timer, mem::GPoolAllocator<Timer> > >;

    //! priority queue of timer callbacks, obviously kept in timeout
    //! order. Currently not addressable.
    TimerPQ timer_pq_;

    /*------------------------------------------------------------------------*/

    //! deque of asynchronous readers
    std::deque<AsyncReadBuffer,
               mem::GPoolAllocator<AsyncReadBuffer> > async_read_;

    //! deque of asynchronous writers
    std::deque<AsyncWriteBuffer,
               mem::GPoolAllocator<AsyncWriteBuffer> > async_write_;

    //! deque of asynchronous readers
    std::deque<AsyncReadByteBlock,
               mem::GPoolAllocator<AsyncReadByteBlock> > async_read_block_;

    //! deque of asynchronous writers
    std::deque<AsyncWriteBlock,
               mem::GPoolAllocator<AsyncWriteBlock> > async_write_block_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_DISPATCHER_HEADER

/******************************************************************************/
