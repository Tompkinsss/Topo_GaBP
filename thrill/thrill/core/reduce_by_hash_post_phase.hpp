/*******************************************************************************
 * thrill/core/reduce_by_hash_post_phase.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_BY_HASH_POST_PHASE_HEADER
#define THRILL_CORE_REDUCE_BY_HASH_POST_PHASE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool VolatileKey,
          typename ReduceConfig_ = DefaultReduceConfig,
          typename IndexFunction = ReduceByHash<Key>,
          typename KeyEqualFunction = std::equal_to<Key> >
class ReduceByHashPostPhase
{
    static constexpr bool debug = false;

public:
    using ReduceConfig = ReduceConfig_;
    using PhaseEmitter = ReducePostPhaseEmitter<
        TableItem, Value, Emitter, VolatileKey>;

    using Table = typename ReduceTableSelect<
        ReduceConfig::table_impl_,
        TableItem, Key, Value,
        KeyExtractor, ReduceFunction, PhaseEmitter,
        VolatileKey, ReduceConfig,
        IndexFunction, KeyEqualFunction>::type;

    /*!
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReduceByHashPostPhase(
        Context& ctx, size_t dia_id,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const Emitter& emit,
        const ReduceConfig& config = ReduceConfig(),
        const IndexFunction& index_function = IndexFunction(),
        const KeyEqualFunction& key_equal_function = KeyEqualFunction())
        : config_(config),
          emitter_(emit),
          table_(ctx, dia_id,
                 key_extractor, reduce_function, emitter_,
                 /* num_partitions */ 32, /* TODO(tb): parameterize */
                 config, /* immediate_flush */ false,
                 index_function, key_equal_function) { }

    //! non-copyable: delete copy-constructor
    ReduceByHashPostPhase(const ReduceByHashPostPhase&) = delete;
    //! non-copyable: delete assignment operator
    ReduceByHashPostPhase& operator = (const ReduceByHashPostPhase&) = delete;

    void Initialize(size_t limit_memory_bytes) {
        table_.Initialize(limit_memory_bytes);
    }

    bool Insert(const TableItem& kv) {
        return table_.Insert(kv);
    }

    //! Flushes all items in the whole table.
    template <bool DoCache>
    void Flush(bool consume, data::File::Writer* writer = nullptr) {
        LOG << "Flushing items";

        // list of remaining files, containing only partially reduced item pairs
        // or items
        std::vector<data::File> remaining_files;

        // read primary hash table, since ReduceByHash delivers items in any
        // order, we can just emit items from fully reduced partitions.

        {
            std::vector<data::File>& files = table_.partition_files();

            for (size_t id = 0; id < files.size(); ++id)
            {
                // get the actual reader from the file
                data::File& file = files[id];

                // if items have been spilled, store for a second reduce
                if (file.num_items() > 0) {
                    table_.SpillPartition(id);

                    LOG << "partition " << id << " contains "
                        << file.num_items() << " partially reduced items";

                    remaining_files.emplace_back(std::move(file));
                }
                else {
                    LOG << "partition " << id << " contains "
                        << table_.items_per_partition(id)
                        << " fully reduced items";

                    table_.FlushPartitionEmit(
                        id, consume, /* grow */ false,
                        [this, writer](
                            const size_t& partition_id, const TableItem& p) {
                            if (DoCache) writer->Put(p);
                            emitter_.Emit(partition_id, p);
                        });
                }
            }
        }

        if (remaining_files.size() == 0) {
            LOG << "Flushed items directly.";
            return;
        }

        table_.Dispose();

        assert(consume && "Items were spilled hence Flushing must consume");

        // if partially reduce files remain, create new hash tables to process
        // them iteratively.

        size_t iteration = 1;

        while (remaining_files.size())
        {
            sLOG << "ReducePostPhase: re-reducing items from"
                 << remaining_files.size() << "spilled files"
                 << "iteration" << iteration;
            sLOG << "-- Try to increase the amount of RAM to avoid this.";

            std::vector<data::File> next_remaining_files;

            Table subtable(
                table_.ctx(), table_.dia_id(),
                table_.key_extractor(), table_.reduce_function(), emitter_,
                /* num_partitions */ 32, config_, /* immediate_flush */ false,
                IndexFunction(iteration, table_.index_function()),
                table_.key_equal_function());

            subtable.Initialize(table_.limit_memory_bytes());

            size_t num_subfile = 0;

            for (data::File& file : remaining_files)
            {
                // insert all items from the partially reduced file
                sLOG << "re-reducing subfile" << num_subfile++
                     << "containing" << file.num_items() << "items";

                data::File::ConsumeReader reader = file.GetConsumeReader();

                while (reader.HasNext()) {
                    subtable.Insert(reader.Next<TableItem>());
                }

                // after insertion, flush fully reduced partitions and save
                // remaining files for next iteration.

                std::vector<data::File>& subfiles = subtable.partition_files();

                for (size_t id = 0; id < subfiles.size(); ++id)
                {
                    // get the actual reader from the file
                    data::File& subfile = subfiles[id];

                    // if items have been spilled, store for a second reduce
                    if (subfile.num_items() > 0) {
                        subtable.SpillPartition(id);

                        sLOG << "partition" << id << "contains"
                             << subfile.num_items() << "partially reduced items";

                        next_remaining_files.emplace_back(std::move(subfile));
                    }
                    else {
                        sLOG << "partition" << id << "contains"
                             << subtable.items_per_partition(id)
                             << "fully reduced items";

                        subtable.FlushPartitionEmit(
                            id, /* consume */ true, /* grow */ false,
                            [this, writer](
                                const size_t& partition_id, const TableItem& p) {
                                if (DoCache) writer->Put(p);
                                emitter_.Emit(partition_id, p);
                            });
                    }
                }
            }

            remaining_files = std::move(next_remaining_files);
            ++iteration;
        }

        LOG << "Flushed items";
    }

    //! Push data into emitter
    void PushData(bool consume = false) {
        if (!cache_)
        {
            if (!table_.has_spilled_data()) {
                // no items were spilled to disk, hence we can emit all data
                // from RAM.
                Flush</* DoCache */ false>(consume);
            }
            else {
                // items were spilled, hence the reduce table must be emptied
                // and we have to cache the output stream.
                cache_ = table_.ctx().GetFilePtr(table_.dia_id());
                data::File::Writer writer = cache_->GetWriter();
                Flush</* DoCache */ true>(true, &writer);
            }
        }
        else
        {
            // previous PushData() has stored data in cache_
            data::File::Reader reader = cache_->GetReader(consume);
            while (reader.HasNext())
                emitter_.Emit(reader.Next<TableItem>());
        }
    }

    void Dispose() {
        table_.Dispose();
        if (cache_) cache_.reset();
    }

    //! \name Accessors
    //! \{

    //! Returns mutable reference to first table_
    Table& table() { return table_; }

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! \}

private:
    //! Stored reduce config to initialize the subtable.
    ReduceConfig config_;

    //! Emitters used to parameterize hash table for output to next DIA node.
    PhaseEmitter emitter_;

    //! the first-level hash table implementation
    Table table_;

    //! File for storing data in-case we need multiple re-reduce levels.
    data::FilePtr cache_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_HASH_POST_PHASE_HEADER

/******************************************************************************/
