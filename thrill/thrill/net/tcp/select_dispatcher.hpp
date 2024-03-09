/*******************************************************************************
 * thrill/net/tcp/select_dispatcher.hpp
 *
 * Asynchronous callback wrapper around select()
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_SELECT_DISPATCHER_HEADER
#define THRILL_NET_TCP_SELECT_DISPATCHER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/connection.hpp>
#include <thrill/net/dispatcher.hpp>
#include <thrill/net/exception.hpp>
#include <thrill/net/tcp/connection.hpp>
#include <thrill/net/tcp/select.hpp>
#include <thrill/net/tcp/socket.hpp>
#include <tlx/delegate.hpp>
#include <tlx/die.hpp>

#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <deque>
#include <functional>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \{

/*!
 * SelectDispatcher is a higher level wrapper for select(). One can register
 * Socket objects for readability and writability checks, buffered reads and
 * writes with completion callbacks, and also timer functions.
 */
class SelectDispatcher final : public net::Dispatcher
{
    static constexpr bool debug = false;

    static constexpr bool self_verify_ = common::g_self_verify;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    //! constructor
    explicit SelectDispatcher() : net::Dispatcher() {
        // allocate self-pipe
        common::MakePipe(self_pipe_);

        if (!Socket::SetNonBlocking(self_pipe_[0], true)) {
            LOG1 << "SelectDispatcher() cannot set up self-pipe for non-blocking reads";
        }

        // Ignore PIPE signals (received when writing to closed sockets)
        signal(SIGPIPE, SIG_IGN);

        // wait interrupts via self-pipe.
        AddRead(self_pipe_[0],
                Callback::make<SelectDispatcher,
                               & SelectDispatcher::SelfPipeCallback>(this));
    }

    //! non-copyable: delete copy-constructor
    SelectDispatcher(const SelectDispatcher&) = delete;
    //! non-copyable: delete assignment operator
    SelectDispatcher& operator = (const SelectDispatcher&) = delete;
    //! move-constructor: default
    SelectDispatcher(SelectDispatcher&&) = default;
    //! move-assignment operator: default
    SelectDispatcher& operator = (SelectDispatcher&&) = default;

    ~SelectDispatcher() {
        ::close(self_pipe_[0]);
        ::close(self_pipe_[1]);
    }

    //! Grow table if needed
    void CheckSize(int fd) {
        assert(fd >= 0);
        assert(fd <= 32000); // this is an arbitrary limit to catch errors.
        if (static_cast<size_t>(fd) >= watch_.size())
            watch_.resize(fd + 1);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(int fd, const Callback& read_cb) {
        CheckSize(fd);
        if (!watch_[fd].read_cb.size()) {
            select_.SetRead(fd);
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].read_cb.emplace_back(read_cb);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& c, const Callback& read_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        return AddRead(fd, read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(net::Connection& c, const Callback& write_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);
        if (!watch_[fd].write_cb.size()) {
            select_.SetWrite(fd);
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].write_cb.emplace_back(write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void SetExcept(net::Connection& c, const Callback& except_cb) {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);
        if (!watch_[fd].except_cb) {
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].except_cb = except_cb;
    }

    //! Cancel all callbacks on a given fd.
    void Cancel(net::Connection& c) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);

        if (watch_[fd].read_cb.size() == 0 &&
            watch_[fd].write_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() fd=" << fd
                << " called with no callbacks registered.";

        select_.ClearRead(fd);
        select_.ClearWrite(fd);
        select_.ClearException(fd);

        Watch& w = watch_[fd];
        w.read_cb.clear();
        w.write_cb.clear();
        w.except_cb = Callback();
        w.active = false;
    }

    //! Run one iteration of dispatching select().
    void DispatchOne(const std::chrono::milliseconds& timeout) final;

    //! Interrupt the current select via self-pipe
    void Interrupt() final;

private:
    //! select() manager object
    Select select_;

    //! self-pipe to wake up select.
    int self_pipe_[2];

    //! buffer to receive one byte signals from self-pipe
    char self_pipe_buffer_[32];

    //! callback vectors per watched file descriptor
    struct Watch {
        //! boolean check whether any callbacks are registered
        bool     active = false;
        //! queue of callbacks for fd.
        std::deque<Callback, mem::GPoolAllocator<Callback> >
                 read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback except_cb;
    };

    //! handlers for all registered file descriptors. the fd integer range
    //! should be small enough, otherwise a more complicated data structure is
    //! needed.
    std::vector<Watch> watch_;

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }

    //! Self-pipe callback
    bool SelfPipeCallback();
};

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_SELECT_DISPATCHER_HEADER

/******************************************************************************/
