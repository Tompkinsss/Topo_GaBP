/*******************************************************************************
 * thrill/net/mpi/group.hpp
 *
 * A Thrill network layer Implementation which uses MPI to transmit messages to
 * peers. Since MPI implementations are very bad at multi-threading, this
 * implementation is not recommended: it sequentialized all calls to the MPI
 * library (such that it does not deadlock), which _requires_ a busy-waiting
 * loop for new messages.
 *
 * Due to this restriction, the mpi::Group allows only **one Thrill host**
 * within a system process. We cannot start independent test threads as MPI
 * would not distinguish them.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MPI_GROUP_HEADER
#define THRILL_NET_MPI_GROUP_HEADER

#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! \addtogroup net_mpi MPI Network API
//! \ingroup net
//! \{

class Group;

/*!
 * A derived exception class which looks up MPI error strings.
 */
class Exception : public net::Exception
{
public:
    explicit Exception(const std::string& what)
        : net::Exception(what) { }

    Exception(const std::string& what, int error_code);

    //! return the MPI error string
    static std::string GetErrorString(int error_code);
};

/*!
 * Virtual MPI connection class. As MPI has no real connections, this class is
 * just the integer which selected an MPI peer. Additionally, it contains the
 * group tag used to separate communication into groups.
 */
class Connection final : public net::Connection
{
    static constexpr bool debug = true;

public:
    //! construct from group tag and MPI peer
    void Initialize(Group* group, int peer) {
        group_ = group;
        peer_ = peer;
    }

    //! \name Base Status Functions
    //! \{

    bool IsValid() const final { return true; }

    //! return the MPI peer number
    int peer() const { return peer_; }

    std::string ToString() const final;

    std::ostream& OutputOstream(std::ostream& os) const final;

    //! \}

    //! \name Send Functions
    //! \{

    void SyncSend(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final;

    ssize_t SendOne(
        const void* data, size_t size, Flags flags = NoFlags) final {
        SyncSend(data, size, flags);
        return size;
    }

    //! \}

    //! \name Receive Functions
    //! \{

    void SyncRecv(void* out_data, size_t size) final;

    ssize_t RecvOne(void* out_data, size_t size) final {
        SyncRecv(out_data, size);
        return size;
    }

    //! \}

    //! \name Paired SendReceive Methods
    //! \{

    void SyncSendRecv(const void* send_data, size_t send_size,
                      void* recv_data, size_t recv_size) final;
    void SyncRecvSend(const void* send_data, size_t send_size,
                      void* recv_data, size_t recv_size) final;

    //! \}

private:
    //! Group reference
    Group* group_;

    //! Outgoing peer id of this Connection.
    int peer_;
};

/*!
 * A net group backed by virtual MPI connection. As MPI already sets up
 * communication, not much is done. Each Group communicates using a unique MPI
 * tag, the group id. Each host's rank within the group is plaining its MPI
 * rank.
 */
class Group final : public net::Group
{
    static constexpr bool debug = true;

public:
    //! \name Base Functions
    //! \{

    //! Initialize a Group for the given size and rank
    Group(size_t my_rank, int group_tag, size_t group_size,
          DispatcherThread& dispatcher)
        : net::Group(my_rank),
          group_tag_(group_tag),
          conns_(group_size),
          dispatcher_(dispatcher) {
        // create virtual connections
        for (size_t i = 0; i < group_size; ++i)
            conns_[i].Initialize(this, static_cast<int>(i));
    }

    //! return MPI tag used to communicate
    int group_tag() const { return group_tag_; }

    //! number of hosts configured.
    size_t num_hosts() const final { return conns_.size(); }

    //! reference to the main MPI dispatcher thread
    DispatcherThread& dispatcher() { return dispatcher_; }

    net::Connection& connection(size_t peer) final {
        assert(peer < conns_.size());
        return conns_[peer];
    }

    void Close() final { }

    //! Number of parallel sends or recvs requests supported by net backend
    size_t num_parallel_async() const final;

    //! Construct a network dispatcher object for the network backend used by
    //! this group, matching its internal implementation. A dispatcher may be
    //! shared between groups of the same type.
    std::unique_ptr<net::Dispatcher> ConstructDispatcher() const final;

    //! run a MPI_Barrier() for synchronization.
    void Barrier();

    //! \}

private:
    //! this group's MPI tag
    int group_tag_;

    //! vector of virtual connection objects to remote peers
    std::vector<Connection> conns_;

    //! reference to the main MPI dispatcher thread
    DispatcherThread& dispatcher_;

    template <typename MpiCall>
    void WaitForRequest(MpiCall call);

    //! \name Virtual Synchronous Collectives to Override Implementations
    //! \{

/*[[[perl
  for my $e (
    ["int", "Int"], ["unsigned int", "UnsignedInt"],
    ["long", "Long"], ["unsigned long", "UnsignedLong"],
    ["long long", "LongLong"], ["unsigned long long", "UnsignedLongLong"])
  {
    print "void PrefixSumPlus$$e[1]($$e[0]& value, const $$e[0]& initial) final;\n";
    print "void ExPrefixSumPlus$$e[1]($$e[0]& value, const $$e[0]& initial) final;\n";
    print "void Broadcast$$e[1]($$e[0]& value, size_t origin) final;\n";
    print "void AllReducePlus$$e[1]($$e[0]& value) final;\n";
    print "void AllReduceMinimum$$e[1]($$e[0]& value) final;\n";
    print "void AllReduceMaximum$$e[1]($$e[0]& value) final;\n";
  }
]]]*/
    void PrefixSumPlusInt(int& value, const int& initial) final;
    void ExPrefixSumPlusInt(int& value, const int& initial) final;
    void BroadcastInt(int& value, size_t origin) final;
    void AllReducePlusInt(int& value) final;
    void AllReduceMinimumInt(int& value) final;
    void AllReduceMaximumInt(int& value) final;
    int InterMapPlusInt(int& value) final;
    void PrefixSumPlusUnsignedInt(unsigned int& value, const unsigned int& initial) final;
    void ExPrefixSumPlusUnsignedInt(unsigned int& value, const unsigned int& initial) final;
    void BroadcastUnsignedInt(unsigned int& value, size_t origin) final;
    void AllReducePlusUnsignedInt(unsigned int& value) final;
    void AllReduceMinimumUnsignedInt(unsigned int& value) final;
    void AllReduceMaximumUnsignedInt(unsigned int& value) final;
    void PrefixSumPlusLong(long& value, const long& initial) final;
    void ExPrefixSumPlusLong(long& value, const long& initial) final;
    void BroadcastLong(long& value, size_t origin) final;
    void AllReducePlusLong(long& value) final;
    void AllReduceMinimumLong(long& value) final;
    void AllReduceMaximumLong(long& value) final;
    void PrefixSumPlusUnsignedLong(unsigned long& value, const unsigned long& initial) final;
    void ExPrefixSumPlusUnsignedLong(unsigned long& value, const unsigned long& initial) final;
    void BroadcastUnsignedLong(unsigned long& value, size_t origin) final;
    void AllReducePlusUnsignedLong(unsigned long& value) final;
    void AllReduceMinimumUnsignedLong(unsigned long& value) final;
    void AllReduceMaximumUnsignedLong(unsigned long& value) final;
    void PrefixSumPlusLongLong(long long& value, const long long& initial) final;
    void ExPrefixSumPlusLongLong(long long& value, const long long& initial) final;
    void BroadcastLongLong(long long& value, size_t origin) final;
    void AllReducePlusLongLong(long long& value) final;
    void AllReduceMinimumLongLong(long long& value) final;
    void AllReduceMaximumLongLong(long long& value) final;
    void PrefixSumPlusUnsignedLongLong(unsigned long long& value, const unsigned long long& initial) final;
    void ExPrefixSumPlusUnsignedLongLong(unsigned long long& value, const unsigned long long& initial) final;
    void BroadcastUnsignedLongLong(unsigned long long& value, size_t origin) final;
    void AllReducePlusUnsignedLongLong(unsigned long long& value) final;
    void AllReduceMinimumUnsignedLongLong(unsigned long long& value) final;
    void AllReduceMaximumUnsignedLongLong(unsigned long long& value) final;
// [[[end]]]

    //! \}
};

/*!
 * Construct Group which connects to peers using MPI. As the MPI environment
 * already defines the connections, no hosts or parameters can be
 * given. Constructs group_count mpi::Group objects at once. Within each Group
 * this host has its MPI rank.
 *
 * To enable tests with smaller group sizes, the Construct method takes
 * group_size and returns a Group with *less* hosts than actual MPI processes!
 * Obviously, group_size must be less-or-equal to the number of processes
 * started with mpirun -np.
 */
bool Construct(size_t group_size, DispatcherThread& dispatcher,
               std::unique_ptr<Group>* groups, size_t group_count);

/*!
 * Return the number of MPI processes. This is the maximum group size.
 */
size_t NumMpiProcesses();

//! Return the rank of this process in the MPI COMM WORLD.
size_t MpiRank();

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_GROUP_HEADER

/******************************************************************************/
