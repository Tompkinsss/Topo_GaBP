/*******************************************************************************
 * thrill/core/reduce_old_probing_hash_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_OLD_PROBING_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_OLD_PROBING_HASH_TABLE_HEADER

#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_table.hpp>

#include <tlx/vector_free.hpp>

#include <functional>
#include <limits>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

/*!
 * A data structure which takes an arbitrary value and extracts a key using a
 * key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value
 * pair to some slot.
 *
 * In case a slot already has a key/value pair and the key of that value and the
 * key of the value to be inserted are them same, the values are reduced
 * according to some reduce function. No key/value is added to the data
 * structure.
 *
 * If the keys are different, the next slot (moving to the right) is considered.
 * If the slot is occupied, the same procedure happens again (know as linear
 * probing.)
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot.
 * 3.) Trigger a resize of the data structure in case there are no more free
 *     slots in the data structure.
 *
 * The following illustrations shows the general structure of the data
 * structure.  The set of slots is divided into 1..n partitions. Each key is
 * hashed into exactly one partition.
 *
 *
 *     Partition 0 Partition 1 Partition 2 Partition 3 Partition 4
 *     P00 P01 P02 P10 P11 P12 P20 P21 P22 P30 P31 P32 P40 P41 P42
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    ||  |   |   ||  |   |   ||  |   |   ||  |   |   ||  |   |  ||
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *                <-   LI  ->
 *                     LI..Local Index
 *    <-        GI         ->
 *              GI..Global Index
 *         PI 0        PI 1        PI 2        PI 3        PI 4
 *         PI..Partition ID
 *
 */
template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool VolatileKey,
          typename ReduceConfig_,
          typename IndexFunction,
          typename KeyEqualFunction = std::equal_to<Key> >
class ReduceOldProbingHashTable
    : public ReduceTable<TableItem, Key, Value,
                         KeyExtractor, ReduceFunction, Emitter,
                         VolatileKey, ReduceConfig_,
                         IndexFunction, KeyEqualFunction>
{
    using Super = ReduceTable<TableItem, Key, Value,
                              KeyExtractor, ReduceFunction, Emitter,
                              VolatileKey, ReduceConfig_, IndexFunction,
                              KeyEqualFunction>;
    using Super::debug;
    static constexpr bool debug_items = false;

public:
    using ReduceConfig = ReduceConfig_;
    using TableItemIterator = typename std::vector<TableItem>::iterator;

    ReduceOldProbingHashTable(
        Context& ctx, size_t dia_id,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        Emitter& emitter,
        size_t num_partitions,
        const ReduceConfig& config = ReduceConfig(),
        bool immediate_flush = false,
        const IndexFunction& index_function = IndexFunction(),
        const KeyEqualFunction& key_equal_function = KeyEqualFunction())
        : Super(ctx, dia_id,
                key_extractor, reduce_function, emitter,
                num_partitions, config, immediate_flush,
                index_function, key_equal_function) {

        assert(num_partitions > 0);
    }

    //! Construct the hash table itself. fill it with sentinels. have one extra
    //! cell beyond the end for reducing the sentinel itself.
    void Initialize(size_t limit_memory_bytes) {

        limit_memory_bytes_ = limit_memory_bytes;

        // calculate num_buckets_per_partition_ from the memory limit and the
        // number of partitions required

        assert(limit_memory_bytes_ >= 0 &&
               "limit_memory_bytes must be greater than or equal to 0. "
               "A byte size of zero results in exactly one item per partition");

        num_buckets_per_partition_ = std::max<size_t>(
            1,
            (size_t)(static_cast<double>(limit_memory_bytes_)
                     / static_cast<double>(sizeof(TableItem))
                     / static_cast<double>(num_partitions_)));

        num_buckets_ = num_buckets_per_partition_ * num_partitions_;

        assert(num_buckets_per_partition_ > 0);
        assert(num_buckets_ > 0);

        // calculate limit on the number of items in a partition before these
        // are spilled to disk or flushed to network.

        double limit_fill_rate = config_.limit_partition_fill_rate();

        assert(limit_fill_rate >= 0.0 && limit_fill_rate <= 1.0
               && "limit_partition_fill_rate must be between 0.0 and 1.0. "
               "with a fill rate of 0.0, items are immediately flushed.");

        limit_items_per_partition_ = (size_t)(
            static_cast<double>(num_buckets_per_partition_) * limit_fill_rate);

        assert(limit_items_per_partition_ >= 0);

        // actually allocate the table

        items_.resize(num_buckets_ + 1);
    }

    /*!
     * Inserts a value into the table, potentially reducing it in case both the
     * key of the value already in the table and the key of the value to be
     * inserted are the same.
     *
     * An insert may trigger a partial flush of the partition with the most
     * items if the maximal number of items in the table (max_num_items_table)
     * is reached.
     *
     * Alternatively, it may trigger a resize of the table in case the maximal
     * fill ratio per partition is reached.
     *
     * \param kv Value to be inserted into the table.
         *
         * \return true if a new key was inserted to the table
     */
    bool Insert(const TableItem& kv) {

        while (TLX_UNLIKELY(mem::memory_exceeded && num_items_ != 0))
            SpillAnyPartition();

        typename IndexFunction::Result h = calculate_index(kv);

        assert(h.partition_id < num_partitions_);

        if (key_equal_function_(key(kv), Key())) {
            // handle pairs with sentinel key specially by reducing into last
            // element of items.
            TableItem& sentinel = items_[num_buckets_];
            if (sentinel_partition_ == invalid_partition_) {
                // first occurrence of sentinel key
                sentinel = kv;
                sentinel_partition_ = h.partition_id;
            }
            else {
                sentinel = reduce(sentinel, kv);
            }
            ++items_per_partition_[h.partition_id];
            ++num_items_;

            while (items_per_partition_[h.partition_id] > limit_items_per_partition_)
                SpillPartition(h.partition_id);

            return false;
        }

        size_t local_index = h.local_index(num_buckets_per_partition_);

        TableItemIterator pbegin =
            items_.begin() + h.partition_id * num_buckets_per_partition_;
        TableItemIterator pend = pbegin + num_buckets_per_partition_;

        TableItemIterator begin_iter = pbegin + local_index;
        TableItemIterator iter = begin_iter;

        while (!key_equal_function_(key(*iter), Key()))
        {
            if (key_equal_function_(key(*iter), key(kv)))
            {
                *iter = reduce(*iter, kv);
                return false;
            }

            ++iter;

            // wrap around if beyond the current partition
            if (iter == pend)
                iter = pbegin;

            // flush partition, if all slots are reserved
            if (iter == begin_iter) {

                SpillPartition(h.partition_id);

                *iter = kv;

                // increase counter for partition
                ++items_per_partition_[h.partition_id];
                ++num_items_;

                return true;
            }
        }

        // insert new pair
        *iter = kv;

        // increase counter for partition
        ++items_per_partition_[h.partition_id];
        ++num_items_;

        while (items_per_partition_[h.partition_id] > limit_items_per_partition_)
            SpillPartition(h.partition_id);

        return true;
    }

    //! Deallocate memory
    void Dispose() {
        tlx::vector_free(items_);
        Super::Dispose();
    }

    //! \name Spilling Mechanisms to External Memory Files
    //! \{

    //! Spill all items of a partition into an external memory File.
    void SpillPartition(size_t partition_id) {

        if (immediate_flush_) {
            return FlushPartition(
                partition_id, /* consume */ true, /* grow */ true);
        }

        LOG << "Spilling " << items_per_partition_[partition_id]
            << " items of partition with id: " << partition_id;

        if (items_per_partition_[partition_id] == 0)
            return;

        data::File::Writer writer = partition_files_[partition_id].GetWriter();

        if (sentinel_partition_ == partition_id) {
            writer.Put(items_[num_buckets_]);
            items_[num_buckets_] = TableItem();
            sentinel_partition_ = invalid_partition_;
        }

        TableItemIterator iter =
            items_.begin() + partition_id * num_buckets_per_partition_;
        TableItemIterator end =
            items_.begin() + (partition_id + 1) * num_buckets_per_partition_;

        for ( ; iter != end; ++iter)
        {
            if (!key_equal_function_(key(*iter), Key()))
            {
                writer.Put(*iter);
                *iter = TableItem();
            }
        }

        // reset partition specific counter
        num_items_ -= items_per_partition_[partition_id];
        items_per_partition_[partition_id] = 0;
        assert(num_items_ == this->num_items_calc());

        LOG << "Spilled items of partition with id: " << partition_id;
    }

    //! Spill all items of an arbitrary partition into an external memory File.
    void SpillAnyPartition() {
        // maybe make a policy later -tb
        return SpillLargestPartition();
    }

    //! Spill all items of the largest partition into an external memory File.
    void SpillLargestPartition() {
        // get partition with max size
        size_t size_max = 0, index = 0;

        for (size_t i = 0; i < num_partitions_; ++i)
        {
            if (items_per_partition_[i] > size_max)
            {
                size_max = items_per_partition_[i];
                index = i;
            }
        }

        if (size_max == 0) {
            return;
        }

        return SpillPartition(index);
    }

    //! \}

    //! \name Flushing Mechanisms to Next Stage or Phase
    //! \{

    template <typename Emit>
    void FlushPartitionEmit(
        size_t partition_id, bool consume, bool /* grow */, Emit emit) {

        LOG << "Flushing " << items_per_partition_[partition_id]
            << " items of partition: " << partition_id;

        if (sentinel_partition_ == partition_id) {
            emit(partition_id, items_[num_buckets_]);
            if (consume) {
                items_[num_buckets_] = TableItem();
                sentinel_partition_ = invalid_partition_;
            }
        }

        TableItemIterator iter =
            items_.begin() + partition_id * num_buckets_per_partition_;
        TableItemIterator end =
            items_.begin() + (partition_id + 1) * num_buckets_per_partition_;

        for ( ; iter != end; ++iter)
        {
            if (!key_equal_function_(key(*iter), Key())) {
                emit(partition_id, *iter);

                if (consume)
                    *iter = TableItem();
            }
        }

        if (consume) {
            // reset partition specific counter
            num_items_ -= items_per_partition_[partition_id];
            items_per_partition_[partition_id] = 0;
            assert(num_items_ == this->num_items_calc());
        }

        LOG << "Done flushed items of partition: " << partition_id;
    }

    void FlushPartition(size_t partition_id, bool consume, bool grow) {
        FlushPartitionEmit(
            partition_id, consume, grow,
            [this](const size_t& partition_id, const TableItem& p) {
                this->emitter_.Emit(partition_id, p);
            });
    }

    void FlushAll() {
        for (size_t i = 0; i < num_partitions_; ++i) {
            FlushPartition(i, /* consume */ true, /* grow */ false);
        }
    }

    //! \}

public:
    using Super::calculate_index;

private:
    using Super::config_;
    using Super::key_equal_function_;
    using Super::immediate_flush_;
    using Super::index_function_;
    using Super::items_per_partition_;
    using Super::key;
    using Super::limit_items_per_partition_;
    using Super::limit_memory_bytes_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::num_items_;
    using Super::num_partitions_;
    using Super::partition_files_;
    using Super::reduce;

    //! Storing the actual hash table.
    std::vector<TableItem> items_;

    //! sentinel for invalid partition or no sentinel.
    static constexpr size_t invalid_partition_ = size_t(-1);

    //! store the partition id of the sentinel key. implicitly this also stored
    //! whether the sentinel key was found and reduced into
    //! items_[num_buckets_].
    size_t sentinel_partition_ = invalid_partition_;
};

template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          typename Emitter, const bool VolatileKey,
          typename ReduceConfig, typename IndexFunction,
          typename KeyEqualFunction>
class ReduceTableSelect<
        ReduceTableImpl::OLD_PROBING,
        TableItem, Key, Value, KeyExtractor, ReduceFunction,
        Emitter, VolatileKey, ReduceConfig, IndexFunction, KeyEqualFunction>
{
public:
    using type = ReduceOldProbingHashTable<
        TableItem, Key, Value, KeyExtractor, ReduceFunction,
        Emitter, VolatileKey, ReduceConfig,
        IndexFunction, KeyEqualFunction>;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_OLD_PROBING_HASH_TABLE_HEADER

/******************************************************************************/
