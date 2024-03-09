/*******************************************************************************
 * thrill/common/concurrent_queue.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONCURRENT_QUEUE_HEADER
#define THRILL_COMMON_CONCURRENT_QUEUE_HEADER

#include <atomic>
#include <deque>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * This is a queue, similar to std::queue and tbb::concurrent_queue, except that
 * it uses mutexes for synchronization.
 *
 * StyleGuide is violated, because signatures are expected to match those of
 * std::queue.
 */
template <typename T, typename Allocator>
class ConcurrentQueue
{
public:
    using value_type = T;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

private:
    //! the actual data queue
    std::deque<T, Allocator> queue_;

    //! the mutex to lock before accessing the queue
    mutable std::mutex mutex_;

public:
    //! Constructor
    explicit ConcurrentQueue(const Allocator& alloc = Allocator())
        : queue_(alloc) { }

    //! Pushes a copy of source onto back of the queue.
    void push(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(source);
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void push(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(std::move(elem));
    }

    //! Pushes a new element into the queue. The element is constructed with
    //! given arguments.
    template <typename... Arguments>
    void emplace(Arguments&& ... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.emplace_back(std::forward<Arguments>(args) ...);
    }

    //! Returns: true if queue has no items; false otherwise.
    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    //! If value is available, pops it from the queue, assigns it to
    //! destination, and destroys the original value. Otherwise does nothing.
    bool try_pop(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        destination = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    //! Clears the queue.
    void clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.clear();
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONCURRENT_QUEUE_HEADER

/******************************************************************************/
