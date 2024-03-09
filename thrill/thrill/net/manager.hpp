/*******************************************************************************
 * thrill/net/manager.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MANAGER_HEADER
#define THRILL_NET_MANAGER_HEADER

#include <thrill/common/json_logger.hpp>
#include <thrill/common/profile_task.hpp>
#include <thrill/net/connection.hpp>
#include <thrill/net/group.hpp>

#include <array>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

struct Traffic {
    //! transmitted bytes
    size_t tx;
    //! received bytes
    size_t rx;
    //! both transmitted and received bytes
    size_t total() const { return tx + rx; }
    //! constructor
    Traffic(size_t tx, size_t rx) : tx(tx), rx(rx) { }
    //! formatting: print total
    friend std::ostream& operator << (std::ostream& os, const Traffic& t);
};

/*!
 * Initializes communication channels, manages communication channels and
 * handles errors.
 *
 * \details This class is responsible for initializing the three net::Groups for
 * the major network components, SystemControl, FlowControl and DataManagement,
 */
class Manager final : public common::ProfileTask
{
    static constexpr bool debug = false;

public:
    /*!
     * The count of net::Groups to initialize.
     */
    static constexpr size_t kGroupCount = 2;

    size_t my_host_rank() const {
        return groups_[0]->my_host_rank();
    }

    size_t num_hosts() const {
        return groups_[0]->num_hosts();
    }

    //! non-copyable: delete copy-constructor
    Manager(const Manager&) = delete;
    //! non-copyable: delete assignment operator
    Manager& operator = (const Manager&) = delete;

    //! Construct Manager from already initialized net::Groups.
    Manager(std::array<GroupPtr, kGroupCount>&& groups,
            common::JsonLogger& logger) noexcept;

    //! Construct Manager from already initialized net::Groups.
    Manager(std::vector<GroupPtr>&& groups,
            common::JsonLogger& logger) noexcept;

    //! Returns the net::Group for the flow control channel.
    Group& GetFlowGroup() {
        return *groups_[0];
    }

    //! Returns the net::Group for the data manager.
    Group& GetDataGroup() {
        return *groups_[1];
    }

    void Close();

    //! calculate overall traffic for final stats
    net::Traffic Traffic() const;

    //! \name Methods for ProfileTask
    //! \{

    void RunTask(const std::chrono::steady_clock::time_point& tp) final;

    //! \}

private:
    //! The Groups initialized and managed by this Manager.
    std::array<GroupPtr, kGroupCount> groups_;

    //! JsonLogger for statistics output
    common::JsonLogger& logger_;

    //! last time statistics where outputted
    std::chrono::steady_clock::time_point tp_last_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MANAGER_HEADER

/******************************************************************************/
