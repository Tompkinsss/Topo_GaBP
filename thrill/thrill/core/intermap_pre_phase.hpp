/*******************************************************************************
 * thrill/core/reduce_pre_phase.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_INTERMAP_PRE_PHASE_HEADER
#define THRILL_CORE_INTERMAP_PRE_PHASE_HEADER

#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/core/duplicate_detection.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>
#include <iostream>

namespace thrill {
namespace core {

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the pre-phase are
//! transmitted via a network Channel.
template <typename ValueType, typename BlockWriter>
class InterMapPrePhaseEmitter
{
    static constexpr bool debug = false;

public:
    explicit InterMapPrePhaseEmitter(std::vector<BlockWriter>& writer)
        : writer_(writer),
          stats_(writer.size(), 0) { }

    //! output an element into a partition, template specialized for robust and
    //! non-robust keys
    void Emit(const size_t& partition_id, const TableItem& p) {
        assert(partition_id < writer_.size());
        stats_[partition_id]++;
        writer_[partition_id].Put(p);

    }

    void Flush(size_t partition_id) {

        assert(partition_id < writer_.size());
        writer_[partition_id].Flush();
    }

    void CloseAll() {
        sLOG << "emit stats:";
        size_t i = 0;
        for (BlockWriter& e : writer_) {
            e.Close();
            sLOG << "emitter" << i << "pushed" << stats_[i++];
        }
    }

public:
    //! Set of emitters, one per partition.
    std::vector<BlockWriter>& writer_;

    //! Emitter stats.
    std::vector<size_t> stats_;
};

template <typename ValueType, typename BlockWriter>
class InterMapPrePhase;

template <typename ValueType, typename BlockWriter>
class InterMapPrePhase<ValueType, BlockWriter>
{
    static constexpr bool debug = false;

public:
    using Emitter = InterMapPrePhaseEmitter<ValueType, BlockWriter>;

    InterMapPrePhase(Context& ctx, size_t dia_id,
                   size_t num_partitions,
                   std::vector<BlockWriter>& emit)
        : emit_(emit) {

        assert(num_partitions == emit.size());
    }

    //! non-copyable: delete copy-constructor
    InterMapPrePhase(const InterMapPrePhase&) = delete;
    //! non-copyable: delete assignment operator
    InterMapPrePhase& operator = (const InterMapPrePhase&) = delete;

    bool Insert(const ValueType& v, bool isFirst, bool isLast) {
        // for VolatileKey this makes std::pair and extracts the key

        if(isFirst){
            first_values_.push_back(v);
        }
        if(isLast){
            last_values_.push_back(v);
        }
        current_values_.push_back(v);
        bool result =  true;

        return result;
    }

    //! Flush all partitions
    void FlushAll() {
        if(my_rank > 0){
            flush(my_rank-1,first_values_);
        }
        if(my_rank < max_rank){
            flush(my_rank+1,last_values_);
        }
    }

    //! Closes all emitter
    void CloseAll() {
        emit_.CloseAll();
    }

    //! \name Accessors
    //! \{

    //! Returns the total num of items in the table.
    size_t num_items() const { return 0; }


    //! \}

protected:
    Emitter emit_;

    std::vector<ValueType> first_values_;
    std::vector<ValueType> current_values_;
    std::vector<ValueType> last_values_;
    
    size_t my_rank_;
    size_t max_rank_;
};
} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PHASE_HEADER

/******************************************************************************/
