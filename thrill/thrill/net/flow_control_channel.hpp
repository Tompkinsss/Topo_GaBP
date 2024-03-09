/*******************************************************************************
 * thrill/net/flow_control_channel.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER
#define THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <thrill/common/defines.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/thread_barrier.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <array>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <iostream>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

/*!
 * Provides a blocking collection for communication.
 *
 * This wraps a raw net group, adds multi-worker/thread support, and should be
 * used for flow control with integral types.
 *
 * Important notice on threading: It is not allowed to call two different
 * methods of two different instances of FlowControlChannel simultaniously by
 * different threads, since the internal synchronization state (the barrier) is
 * shared globally.
 */
class FlowControlChannel
{
private:
    static constexpr bool debug = false;
    static constexpr bool enable_stats = false;

    //! The group associated with this channel.
    Group& group_;

    //! The local host rank.
    size_t host_rank_;

    //! The count of all workers connected to this group.
    size_t num_hosts_;

    //! The id of the worker thread associated with this flow channel.
    size_t local_id_;

    //! The count of all workers connected to this group.
    size_t thread_count_;

    //! Timer or FakeTimer
    using Timer = common::StatsTimerBaseStopped<enable_stats>;
    //! RIAA class for running the timer
    using RunTimer = common::RunTimer<Timer>;

    //! Synchronization timer
    Timer timer_prefixsum_;
    Timer timer_broadcast_;
    Timer timer_reduce_;
    Timer timer_allreduce_;
    Timer timer_predecessor_;
    Timer timer_barrier_;

    Timer timer_communication_;

    //! Synchronization counters
    common::AtomicMovable<size_t> count_prefixsum_ { 0 };
    common::AtomicMovable<size_t> count_broadcast_ { 0 };
    common::AtomicMovable<size_t> count_reduce_ { 0 };
    common::AtomicMovable<size_t> count_allreduce_ { 0 };
    common::AtomicMovable<size_t> count_predecessor_ { 0 };
    common::AtomicMovable<size_t> count_successor_ { 0 };
    common::AtomicMovable<size_t> count_barrier_ { 0 };

    //! The shared barrier used to synchronize between worker threads on this
    //! node.
    common::ThreadBarrier& barrier_;

    //! Thread local data structure: aligned such that no cache line is
    //! shared. The actual vector is in the FlowControlChannelManager.
    class LocalData
    {
    public:
        //! pointer to some thread-owned data type
        alignas(common::g_cache_line_size)
        std::atomic<void*> ptr[2] = { { nullptr }, { nullptr } };

        //! atomic generation counter, compare this to generation_.
        std::atomic<size_t> counter { 0 };

#if THRILL_HAVE_THREAD_SANITIZER
        // workarounds because ThreadSanitizer has false-positives work with
        // generation counters.

        //! mutex for locking condition variable
        std::mutex mutex;

        //! condition variable for signaling incrementing of conunter.
        std::condition_variable cv;
#endif

        //! \name Generation Counting
        //! \{

        void WaitCounter(size_t this_step) {
#if THRILL_HAVE_THREAD_SANITIZER
            std::unique_lock<std::mutex> lock(mutex);
            while (counter != this_step)
                cv.wait(lock);
#else
            // busy wait on generation counter of predecessor
            while (counter.load(std::memory_order_relaxed) != this_step) { }
#endif
        }

        void IncCounter() {
            ++counter;
#if THRILL_HAVE_THREAD_SANITIZER
            std::unique_lock<std::mutex> lock(mutex);
            cv.notify_one();
#endif
        }

        //! \}
    };

    static_assert(sizeof(LocalData) % common::g_cache_line_size == 0,
                  "struct LocalData has incorrect size.");

    //! for access to struct LocalData
    friend class FlowControlChannelManager;

    //! The global shared local data memory location to work upon.
    LocalData* shmem_;

    //! Host-global shared generation counter
    std::atomic<size_t>& generation_;

    //! \name Pointer Casting
    //! \{

    size_t GetNextStep() {
        return (barrier_.step() + 1) % 2;
    }

    template <typename T>
    void SetLocalShared(size_t step, const T* value) {
        // We are only allowed to set our own memory location.
        size_t idx = local_id_;
        shmem_[idx].ptr[step].store(
            const_cast<void*>(reinterpret_cast<const void*>(value)),
            std::memory_order_release);
    }

    template <typename T>
    T * GetLocalShared(size_t step, size_t idx) {
        assert(idx < thread_count_);
        return reinterpret_cast<T*>(
            shmem_[idx].ptr[step].load(std::memory_order_acquire));
    }

    template <typename T>
    T * GetLocalShared(size_t step) {
        GetLocalShared<T>(step, local_id_);
    }

    //! \}

public:
    //! Creates a new instance of this class, wrapping a net::Group.
    FlowControlChannel(
        Group& group, size_t local_id, size_t thread_count,
        common::ThreadBarrier& barrier, LocalData* shmem,
        std::atomic<size_t>& generation);

    //! Return the associated net::Group. USE AT YOUR OWN RISK.
    Group& group() { return group_; }

    //! Return the worker's global rank
    size_t my_rank() const {
        return group_.my_host_rank() * thread_count_ + local_id_;
    }

    //! Return the total number of workers.
    size_t num_workers() const {
        return group_.num_hosts() * thread_count_;
    }

    //! non-copyable: delete copy-constructor
    FlowControlChannel(const FlowControlChannel&) = delete;
    //! non-copyable: delete assignment operator
    FlowControlChannel& operator = (const FlowControlChannel&) = delete;
    //! move-constructor: default
    FlowControlChannel(FlowControlChannel&&) = default;

    ~FlowControlChannel();

#ifdef SWIG
#define TLX_ATTRIBUTE_WARN_UNUSED_RESULT
#endif

    /*!
     * Calculates the prefix sum over all workers, given a certain sum
     * operation.
     *
     * This method blocks until the sum is caluclated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param initial The initial element for the body defined by T and sum_op
     * \param sum_op The operation to use for
     * calculating the prefix sum. The default operation is a normal addition.
     * \param inclusive Whether the prefix sum is inclusive or exclusive.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    PrefixSumBase(const T& value, const BinarySumOp& sum_op = BinarySumOp(),
                  const T& initial = T(), bool inclusive = true) {

        RunTimer run_timer(timer_prefixsum_);
        if (enable_stats || debug) ++count_prefixsum_;
        LOG << "FCC::PrefixSum() ENTER count=" << count_prefixsum_;

        T local_value = value;

        size_t step = GetNextStep();

        SetLocalShared(step, &local_value);

        barrier_.wait(
            [&]() {
                RunTimer net_timer(timer_communication_);

                LOG << "FCC::PrefixSum() COMMUNICATE BEGIN"
                    << " count=" << count_prefixsum_;

                // global prefix
                T** locals = reinterpret_cast<T**>(alloca(thread_count_ * sizeof(T*)));

                for (size_t i = 0; i < thread_count_; i++) {
                    locals[i] = GetLocalShared<T>(step, i);
                }

                T local_sum = *(locals[0]);
                for (size_t i = 1; i < thread_count_; i++) {
                    *(locals[i]) = local_sum = sum_op(local_sum, *(locals[i]));
                }

                T base_sum = local_sum;
                group_.ExPrefixSum(base_sum, sum_op, initial);

                if (inclusive) {
                    for (size_t i = 0; i < thread_count_; i++) {
                        *(locals[i]) = sum_op(base_sum, *(locals[i]));
                    }
                }
                else {
                    for (size_t i = thread_count_ - 1; i > 0; i--) {
                        *(locals[i]) = sum_op(base_sum, *(locals[i - 1]));
                    }
                    *(locals[0]) = base_sum;
                }

                LOG << "FCC::PrefixSum() COMMUNICATE END"
                    << " count=" << count_prefixsum_;
            });

        LOG << "FCC::PrefixSum() EXIT count=" << count_prefixsum_;

        return local_value;
    }

    /*!
     * Calculates the inclusive prefix sum over all workers, given a certain sum
     * operation.
     *
     * This method blocks until the sum is caluclated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param sum_op The operation to use for
     * \param initial The initial element of the body defined by T and SumOp
     * calculating the prefix sum. The default operation is a normal addition.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    PrefixSum(const T& value, const BinarySumOp& sum_op = BinarySumOp(),
              const T& initial = T()) {
        return PrefixSumBase(value, sum_op, initial, true);
    }

    /*!
     * Calculates the exclusive prefix sum over all workers, given a certain sum
     * operation.
     *
     * This method blocks until the sum is calculated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param sum_op The operation to use for
     * \param initial The initial element of the body defined by T and SumOp
     * calculating the prefix sum. The default operation is a normal addition.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    ExPrefixSum(const T& value, const BinarySumOp& sum_op = BinarySumOp(),
                const T& initial = T()) {
        return PrefixSumBase(value, sum_op, initial, false);
    }

    /*!
     * Calculates the exclusive prefix sum over all workers, and delivers the
     * total sum as well. The input value parameter is set to the PE's exclusive
     * prefix sum value and the total sum is returned.
     *
     * This method blocks until the sum is calculated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param sum_op The operation to use for
     * \param initial The initial element of the body defined by T and SumOp
     * calculating the prefix sum. The default operation is a normal addition.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    ExPrefixSumTotal(T& value, const BinarySumOp& sum_op = BinarySumOp(),
                     const T& initial = T()) {

        RunTimer run_timer(timer_prefixsum_);
        if (enable_stats || debug) ++count_prefixsum_;
        LOG << "FCC::ExPrefixSumTotal() ENTER count=" << count_prefixsum_;

        using Result = std::pair<T*, T>;

        Result result { &value, initial };
        size_t step = GetNextStep();
        SetLocalShared(step, &result);

        barrier_.wait(
            [&]() {
                RunTimer net_timer(timer_communication_);

                LOG << "FCC::ExPrefixSumTotal() COMMUNICATE BEGIN"
                    << " count=" << count_prefixsum_;

                Result** locals = reinterpret_cast<Result**>(
                    alloca(thread_count_ * sizeof(Result*)));

                for (size_t i = 0; i < thread_count_; ++i) {
                    locals[i] = GetLocalShared<Result>(step, i);
                }

                T local_sum = *(locals[0]->first);
                for (size_t i = 1; i < thread_count_; ++i) {
                    local_sum = sum_op(local_sum, *(locals[i]->first));
                    *(locals[i]->first) = local_sum;
                }

                T base_sum = local_sum;
                group_.ExPrefixSum(base_sum, sum_op, initial);

                T total_sum;
                if (host_rank_ + 1 == num_hosts_)
                    total_sum = sum_op(base_sum, local_sum);
                group_.Broadcast(total_sum, num_hosts_ - 1);

                for (size_t i = thread_count_ - 1; i > 0; --i) {
                    *(locals[i]->first) = sum_op(base_sum, *(locals[i - 1]->first));
                    locals[i]->second = total_sum;
                }
                *(locals[0]->first) = base_sum;
                locals[0]->second = total_sum;

                LOG << "FCC::ExPrefixSumTotal() COMMUNICATE END"
                    << " count=" << count_prefixsum_;
            });

        LOG << "FCC::ExPrefixSumTotal() EXIT count=" << count_prefixsum_;

        return result.second;
    }

    /*!
     * Broadcasts a value of a serializable type T from the master (the worker
     * with id 0) to all other workers.
     *
     * This method is blocking on all workers except the master.
     *
     * \param value The value to broadcast. This value is ignored for each
     * worker except the master. We use this signature to keep the decision
     * wether a node is the master transparent.
     *
     * \param origin Worker number to broadcast value from.
     *
     * \return The value sent by the master.
     */
    template <typename T>
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    Broadcast(const T& value, size_t origin = 0) {

        RunTimer run_timer(timer_broadcast_);
        if (enable_stats || debug) ++count_broadcast_;
        LOG << "FCC::Broadcast() ENTER count=" << count_broadcast_;

        T local = value;

        size_t step = GetNextStep();
        SetLocalShared(step, &local);

        // Select primary thread of each node to handle I/O (assumes all hosts
        // has the same number of threads).
        size_t primary_pe = origin % thread_count_;

        if (local_id_ == primary_pe) {
            RunTimer net_timer(timer_communication_);
            group_.Broadcast(local, origin / thread_count_);
        }

        barrier_.wait(
            [&]() {
                LOG << "FCC::Broadcast() COMMUNICATE BEGIN"
                    << " count=" << count_broadcast_;

                // copy from primary PE to all others
                T res = *GetLocalShared<T>(step, primary_pe);
                for (size_t i = 0; i < thread_count_; i++) {
                    *GetLocalShared<T>(step, i) = res;
                }

                LOG << "FCC::Broadcast() COMMUNICATE END"
                    << " count=" << count_broadcast_;
            });

        LOG << "FCC::Broadcast() EXIT count=" << count_broadcast_;

        return local;
    }

    /*!
     * Gathers the value of a serializable type T over all workers and
     * provides result to all workers as a shared pointer to a
     * vector.
     *
     * \param value
     * The value this worker contributes to the allgather operation.
     * \return
     * The result of the allgather operation as a shared pointer to a
     * vector.
     */
    template <typename T>
    std::shared_ptr<std::vector<T> > TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    AllGather(const T& value) {
        RunTimer run_timer(timer_reduce_);
        if (enable_stats) ++count_reduce_;

        using SharedVectorT = std::shared_ptr<std::vector<T> >;

        SharedVectorT sp;
        std::pair<T, SharedVectorT> local(value, sp);

        size_t step = GetNextStep();
        SetLocalShared(step, &local);

        barrier_.wait(
            [&]() {
                RunTimer net_timer(timer_communication_);

                size_t n = num_workers();

                // allocate shared vector of correct final size
                auto local_gather = std::make_shared<std::vector<T> >(n);

                if (tlx::is_power_of_two(group().num_hosts())) {
                    // gather local values and insert at correct final positions in the vector
                    for (size_t i = 0; i < thread_count_; i++) {
                        local_gather->at(thread_count_ * group_.my_host_rank() + i) =
                            GetLocalShared<std::pair<T, SharedVectorT> >(step, i)->first;
                    }

                    // global allgather
                    group_.AllGatherRecursiveDoublingPowerOfTwo(local_gather->data(), thread_count_);
                }
                else {
                    // gather local values and insert at correct final positions in the vector
                    for (size_t i = 0; i < thread_count_; i++) {
                        local_gather->at(i) =
                            GetLocalShared<std::pair<T, SharedVectorT> >(step, i)->first;
                    }

                    // global allgather
                    group_.AllGatherBruck(local_gather->data(), thread_count_);
                }

                // distribute shared pointer to worker threads
                for (size_t i = 0; i < thread_count_; i++) {
                    GetLocalShared<std::pair<T, SharedVectorT> >(step, i)->second = local_gather;
                }
            });

        return local.second;
    }

    /*!
     * Reduces a value of a serializable type T over all workers to the given
     * worker, provided a certain reduce function.
     *
     * This method is blocking. The reduce happens in order as with prefix
     * sum. The operation is assumed to be associative.
     *
     * \param value The value to use for the reduce operation.
     * \param root destination worker of the reduce
     * \param sum_op The operation to use for
     * calculating the reduced value. The default operation is a normal addition.
     * \return The result of the reduce operation.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    Reduce(const T& value, size_t root = 0,
           const BinarySumOp& sum_op = BinarySumOp()) {
        assert(root < num_workers());

        RunTimer run_timer(timer_reduce_);
        if (enable_stats || debug) ++count_reduce_;
        LOG << "FCC::Reduce() ENTER count=" << count_reduce_;

        T local = value;

        size_t step = GetNextStep();
        SetLocalShared(step, &local);

        barrier_.wait(
            [&]() {
                RunTimer net_timer(timer_communication_);

                LOG << "FCC::Reduce() COMMUNICATE BEGIN"
                    << " count=" << count_reduce_;

                // local reduce
                T local_sum = *GetLocalShared<T>(step, 0);
                for (size_t i = 1; i < thread_count_; i++) {
                    local_sum = sum_op(local_sum, *GetLocalShared<T>(step, i));
                }

                // global reduce
                group_.Reduce(local_sum, root / thread_count_, sum_op);

                // set the local value only at the root
                if (root / thread_count_ == group_.my_host_rank())
                    *GetLocalShared<T>(step, root % thread_count_) = local_sum;

                LOG << "FCC::Reduce() COMMUNICATE END"
                    << " count=" << count_reduce_;
            });

        LOG << "FCC::Reduce() EXIT count=" << count_reduce_;

        return local;
    }

    /*!
     * Reduces a value of a serializable type T over all workers given a certain
     * reduce function.
     *
     * This method is blocking. The reduce happens in order as with prefix
     * sum. The operation is assumed to be associative.
     *
     * \param value The value to use for the reduce operation.
     * \param sum_op The operation to use for calculating the reduced value. The
     * default operation is a normal addition.
     * \return The result of the reduce operation.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    AllReduce(const T& value, const BinarySumOp& sum_op = BinarySumOp()) {

        RunTimer run_timer(timer_allreduce_);
        if (enable_stats || debug) ++count_allreduce_;
        LOG << "FCC::AllReduce() ENTER count=" << count_allreduce_;

        T local = value;

        size_t step = GetNextStep();
        SetLocalShared(step, &local);

        barrier_.wait(
            [&]() {
                RunTimer net_timer(timer_communication_);

                LOG << "FCC::AllReduce() COMMUNICATE BEGIN"
                    << " count=" << count_allreduce_;

                // local reduce
                T local_sum = *GetLocalShared<T>(step, 0);
                for (size_t i = 1; i < thread_count_; i++) {
                    local_sum = sum_op(local_sum, *GetLocalShared<T>(step, i));
                }

                // global reduce
                group_.AllReduce(local_sum, sum_op);

                // distribute back to local workers
                for (size_t i = 0; i < thread_count_; i++) {
                    *GetLocalShared<T>(step, i) = local_sum;
                }

                LOG << "FCC::AllReduce() COMMUNICATE END"
                    << " count=" << count_allreduce_;
            });

        LOG << "FCC::AllReduce() EXIT count=" << count_allreduce_;

        return local;
    }

    template <typename T, typename BinarySumOp = std::plus<T> >
    T TLX_ATTRIBUTE_WARN_UNUSED_RESULT
    InterMap(const T& value, const BinarySumOp& sum_op = BinarySumOp()){

    }


    /*!
     * Collects up to k predecessors of type T from preceding PEs. k must be
     * equal on all PEs.
     *
     * Assume each worker has <= k items. Predecessor collects up to the k items
     * from preceding PEs. If the directly preceding PE has fewer than k items,
     * then it waits for its predecessor to deliver items, in the hope to get up
     * to k.
     *
     * This is used by the Window() transformation, but may in future also be
     * useful to get a single predecessor item in other distributed operations.
     */
    template <typename T>
    std::vector<T> Predecessor(size_t k, const std::vector<T>& my_values) {

        RunTimer run_timer(timer_predecessor_);
        if (enable_stats || debug) ++count_predecessor_;
        LOG << "FCC::Predecessor() ENTER count=" << count_predecessor_;

        std::vector<T> result;
        size_t step = GetNextStep();

        // this vector must live beyond the ThreadBarrier.
        std::vector<T> send_values;

        // get generation counter
        size_t this_gen = generation_.load(std::memory_order_acquire) + 1;

        if (my_values.size() >= k) {
            // if we already have k items, then "transmit" them to our preccessor
            if (local_id_ + 1 != thread_count_) {
                SetLocalShared(step, &my_values);
                // release memory inside vector
                std::atomic_thread_fence(std::memory_order_release);
                // increment generation counter to match this_step.
                shmem_[local_id_].IncCounter();
            }
            else if (host_rank_ + 1 != num_hosts_) {
                if (my_values.size() > k) {
                    std::vector<T> send_values_next(my_values.end() - k, my_values.end());
                    group_.SendTo(host_rank_ + 1, send_values_next);
                }
                else {
                    group_.SendTo(host_rank_ + 1, my_values);
                }
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
            else {
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }

            // and wait for the predecessor to deliver its batch
            if (local_id_ != 0) {
                // wait on generation counter of predecessor
                shmem_[local_id_ - 1].WaitCounter(this_gen);

                // acquire memory inside vector
                std::atomic_thread_fence(std::memory_order_acquire);

                std::vector<T>* pre =
                    GetLocalShared<std::vector<T> >(step, local_id_ - 1);

                // copy over only k elements (there may be more or less)
                result = std::vector<T>(
                    pre->size() <= k ? pre->begin() : pre->end() - k, pre->end());
            }
            else if (host_rank_ != 0) {
                group_.ReceiveFrom(host_rank_ - 1, &result);
            }
        }
        else {
            // we don't have k items, wait for our predecessor to send some.
            if (local_id_ != 0) {
                // wait on generation counter of predecessor
                shmem_[local_id_ - 1].WaitCounter(this_gen);

                // acquire memory inside vector
                std::atomic_thread_fence(std::memory_order_acquire);

                std::vector<T>* pre =
                    GetLocalShared<std::vector<T> >(step, local_id_ - 1);

                // copy over only k elements (there may be more)
                result = std::vector<T>(
                    pre->size() <= k ? pre->begin() : pre->end() - k, pre->end());
            }
            else if (host_rank_ != 0) {
                group_.ReceiveFrom(host_rank_ - 1, &result);
            }

            // prepend values we got from our predecessor with local ones, such
            // that they will fill up send_values together with all local items
            size_t fill_size = k - my_values.size();
            send_values.reserve(std::min(k, fill_size + result.size()));
            send_values.insert(
                send_values.end(),
                // copy last fill_size items from res. don't do end - fill_size,
                // because that may result in unsigned wrap-around.
                result.size() <= fill_size ? result.begin() : result.end() - fill_size,
                result.end());
            send_values.insert(send_values.end(),
                               my_values.begin(), my_values.end());
            assert(send_values.size() <= k);

            // now we have k items or at many as we can get, hence, "transmit"
            // them to our successor
            if (local_id_ + 1 != thread_count_) {
                SetLocalShared(step, &send_values);
                // release memory inside vector
                std::atomic_thread_fence(std::memory_order_release);
                // increment generation counter to match this_step.
                shmem_[local_id_].IncCounter();
            }
            else if (host_rank_ + 1 != num_hosts_) {
                group_.SendTo(host_rank_ + 1, send_values);
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
            else {
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
        }

        // await until all threads have retrieved their value.
        barrier_.wait([this]() {
                          LOG << "FCC::Predecessor() COMMUNICATE"
                              << " count=" << count_predecessor_;

                          generation_++;
                      });

        LOG << "FCC::Predecessor() EXIT count=" << count_predecessor_;
        send_values.clear();
	send_values.shrink_to_fit();
        return result;
    }


    /*!
     * Collects up to k predecessors of type T from preceding PEs. k must be
     * equal on all PEs.
     *
     * Assume each worker has <= k items. Predecessor collects up to the k items
     * from preceding PEs. If the directly preceding PE has fewer than k items,
     * then it waits for its predecessor to deliver items, in the hope to get up
     * to k.
     *
     * This is used by the Window() transformation, but may in future also be
     * useful to get a single predecessor item in other distributed operations.
     */
    template <typename T>
    std::vector<T> Successor(size_t k, const std::vector<T>& my_values) {

        RunTimer run_timer(timer_predecessor_);
        if (enable_stats || debug) ++count_successor_;
        LOG << "FCC::Predecessor() ENTER count=" << count_successor_;

        std::vector<T> result;
        size_t step = GetNextStep();

        // this vector must live beyond the ThreadBarrier.
        std::vector<T> send_values;

        // get generation counter
        size_t this_gen = generation_.load(std::memory_order_acquire) + 1;

        if (my_values.size() >= k) {
            // if we already have k items, then "transmit" them to our predecessor
            if (local_id_ > 0) {
                SetLocalShared(step, &my_values);
                // release memory inside vector
                std::atomic_thread_fence(std::memory_order_release);
                // increment generation counter to match this_step.
                shmem_[local_id_].IncCounter();
            }
            else if (host_rank_ > 0) {
                if (my_values.size() > k) {
                    std::vector<T> send_values_next(my_values.begin(), my_values.begin()+k);
                    group_.SendTo(host_rank_ - 1, send_values_next);
                }
                else {
                    group_.SendTo(host_rank_ - 1, my_values);
                }
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
            else {
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }

            // and wait for the successor to deliver its batch
            if (local_id_ + 1 != thread_count_) {
                // wait on generation counter of predecessor
                shmem_[local_id_ + 1].WaitCounter(this_gen);

                // acquire memory inside vector
                std::atomic_thread_fence(std::memory_order_acquire);

                std::vector<T>* pre =
                    GetLocalShared<std::vector<T> >(step, local_id_ + 1);

                // copy over only k elements (there may be more or less)
                result = std::vector<T>(pre->begin(),
                    pre->size() <= k ? pre->end() : pre->begin() + k);
            }
            else if (host_rank_ + 1 != num_hosts_) {
                group_.ReceiveFrom(host_rank_ + 1, &result);
            }
        }
        else {
            // we don't have k items, wait for our predecessor to send some.
            if (local_id_ + 1 != thread_count_) {
                // wait on generation counter of predecessor
                shmem_[local_id_ + 1].WaitCounter(this_gen);

                // acquire memory inside vector
                std::atomic_thread_fence(std::memory_order_acquire);

                std::vector<T>* pre =
                    GetLocalShared<std::vector<T> >(step, local_id_ + 1);

                // copy over only k elements (there may be more)
                result = std::vector<T>(pre->begin(),
                    pre->size() <= k ? pre->end() : pre->begin() + k);
            }
            else if (host_rank_ +1 != num_hosts_) {
                group_.ReceiveFrom(host_rank_ + 1, &result);
            }

            // prepend values we got from our predecessor with local ones, such
            // that they will fill up send_values together with all local items
            size_t fill_size = k - my_values.size();
            send_values.reserve(std::min(k, fill_size + result.size()));
            send_values.insert(
                send_values.begin(),
                // copy first fill_size items from res. don't do end - fill_size,
                // because that may result in unsigned wrap-around.
                result.begin(),result.size() <= fill_size ? result.end() : result.begin() + fill_size);
            send_values.insert(send_values.begin(),
                               my_values.begin(), my_values.end());
            assert(send_values.size() <= k);

            // now we have k items or at many as we can get, hence, "transmit"
            // them to our predeccessor
            if (local_id_ > 0) {
                SetLocalShared(step, &send_values);
                // release memory inside vector
                std::atomic_thread_fence(std::memory_order_release);
                // increment generation counter to match this_step.
                shmem_[local_id_].IncCounter();
            }
            else if (host_rank_ > 0) {
                group_.SendTo(host_rank_ - 1, send_values);
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
            else {
                // increment generation counter for synchronizing
                shmem_[local_id_].IncCounter();
            }
        }

        // await until all threads have retrieved their value.
        barrier_.wait([this]() {
                          LOG << "FCC::Predecessor() COMMUNICATE"
                              << " count=" << count_successor_;

                          generation_++;
                      });

	// todo ********************************************
	send_values.clear();
	send_values.shrink_to_fit();
	
	
	
        LOG << "FCC::Predecessor() EXIT count=" << count_successor_;

        return result;
    }


    //! A trivial global barrier.
    void Barrier();

    //! A trivial local thread barrier
    void LocalBarrier();
};

/******************************************************************************/
// extern templates

#if !defined(_MSC_VER)

extern template size_t FlowControlChannel::PrefixSumBase(
    const size_t&, const std::plus<size_t>&, const size_t&, bool);

extern template std::array<size_t, 2> FlowControlChannel::PrefixSumBase(
    const std::array<size_t, 2>&,
    const common::ComponentSum<std::array<size_t, 2> >&,
    const std::array<size_t, 2>&, bool);
extern template std::array<size_t, 3> FlowControlChannel::PrefixSumBase(
    const std::array<size_t, 3>&,
    const common::ComponentSum<std::array<size_t, 3> >&,
    const std::array<size_t, 3>&, bool);
extern template std::array<size_t, 4> FlowControlChannel::PrefixSumBase(
    const std::array<size_t, 4>&,
    const common::ComponentSum<std::array<size_t, 4> >&,
    const std::array<size_t, 4>&, bool);

extern template size_t FlowControlChannel::ExPrefixSumTotal(
    size_t&, const std::plus<size_t>&, const size_t&);

extern template std::array<size_t, 2> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 2>&,
    const common::ComponentSum<std::array<size_t, 2> >&,
    const std::array<size_t, 2>&);
extern template std::array<size_t, 3> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 3>&,
    const common::ComponentSum<std::array<size_t, 3> >&,
    const std::array<size_t, 3>&);
extern template std::array<size_t, 4> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 4>&,
    const common::ComponentSum<std::array<size_t, 4> >&,
    const std::array<size_t, 4>&);

extern template size_t FlowControlChannel::Broadcast(const size_t&, size_t);

extern template std::array<size_t, 2> FlowControlChannel::Broadcast(
    const std::array<size_t, 2>&, size_t);
extern template std::array<size_t, 3> FlowControlChannel::Broadcast(
    const std::array<size_t, 3>&, size_t);
extern template std::array<size_t, 4> FlowControlChannel::Broadcast(
    const std::array<size_t, 4>&, size_t);

extern template size_t FlowControlChannel::AllReduce(
    const size_t&, const std::plus<size_t>&);

extern template int FlowControlChannel::InterMap(const int&, const std::plus<int>&);

#endif  // !defined(_MSC_VER)

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER

/******************************************************************************/
