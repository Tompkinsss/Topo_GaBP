/*******************************************************************************
 * thrill/api/reduce_by_key.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_BY_KEY_HEADER
#define THRILL_API_REDUCE_BY_KEY_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/core/reduce_by_hash_post_phase.hpp>
#include <thrill/core/reduce_pre_phase.hpp>
#include <tlx/meta/is_std_pair.hpp>

#include <functional>
#include <thread>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

class DefaultReduceConfig : public core::DefaultReduceConfig
{ };

/*!
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a
 * DIA by their key and reduces every key bucket to a single element each. The
 * ReduceNode stores the key_extractor and the reduce_function UDFs. The
 * chainable LOps ahead of the Reduce operation are stored in the Stack. The
 * ReduceNode has the type ValueType, which is the result type of the
 * reduce_function.
 *
 * \tparam ValueType Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the
 *  last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function.
 * \tparam VolatileKey Whether to reuse the key once extracted in during pre reduce
 * (false) or let the post reduce extract the key again (true).
 *
 * \ingroup api_layer
 */
template <typename ValueType,
          typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig, typename KeyHashFunction,
          typename KeyEqualFunction, const bool VolatileKey,
          bool UseDuplicateDetection>
class ReduceNode final : public DOpNode<ValueType>
{
private:
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    using TableItem =
        typename std::conditional<
            VolatileKey, std::pair<Key, ValueType>, ValueType>::type;

    using HashIndexFunction = core::ReduceByHash<Key, KeyHashFunction>;

    static constexpr bool use_mix_stream_ = false;//ReduceConfig::use_mix_stream_;
    static constexpr bool use_post_thread_ = ReduceConfig::use_post_thread_;

    //! Emitter for PostPhase to push elements to next DIA object.
    class Emitter
    {
    public:
        explicit Emitter(ReduceNode* node) : node_(node) { }
        void operator () (const ValueType& item) const
        { return node_->PushItem(item); }

    private:
        ReduceNode* node_;
    };

public:
    /*!
     * Constructor for a ReduceNode. Sets the parent, stack, key_extractor and
     * reduce_function.
     */
    template <typename ParentDIA>
    ReduceNode(const ParentDIA& parent,
               const char* label,
               const KeyExtractor& key_extractor,
               const ReduceFunction& reduce_function,
               const ReduceConfig& config,
               const KeyHashFunction& key_hash_function,
               const KeyEqualFunction& key_equal_function)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() })
        ,mix_stream_(use_mix_stream_ ?
                      parent.ctx().GetNewMixStream(this) : nullptr),
          cat_stream_(use_mix_stream_ ?
                      nullptr : parent.ctx().GetNewCatStream(this)),
          emitters_(use_mix_stream_ ?
                    mix_stream_->GetWriters() : cat_stream_->GetWriters()),
          pre_phase_(
              context_, Super::dia_id(), parent.ctx().num_workers(),
              key_extractor, reduce_function, emitters_, config,
              HashIndexFunction(key_hash_function), key_equal_function,
              key_hash_function),
          post_phase_(
              context_, Super::dia_id(), key_extractor, reduce_function,
              Emitter(this), config,
              HashIndexFunction(key_hash_function), key_equal_function) 
    {
        // Hook PreOp: Locally hash elements of the current DIA onto buckets and
        // reduce each bucket to a single value, afterwards send data to another
        // worker given by the shuffle algorithm.
        auto pre_op_fn = [this](const ValueType& input) {
                             return pre_phase_.Insert(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        // request maximum RAM limit, the value is calculated by StageBuilder,
        // and set as DIABase::mem_limit_.
        return DIAMemUse::Max();
    }

    void StartPreOp(size_t /* parent_index */) final {
        LOG << *this << " running StartPreOp";
        if (!use_post_thread_) {
            // use pre_phase without extra thread
            pre_phase_.Initialize(DIABase::mem_limit_);
        }
        else {
            pre_phase_.Initialize(DIABase::mem_limit_ / 2);
            post_phase_.Initialize(DIABase::mem_limit_ / 2);

            // start additional thread to receive from the channel
            thread_ = common::CreateThread([this] { ProcessChannel(); });
        }
    }

    void StopPreOp(size_t /* parent_index */) final {
        LOG << *this << " running StopPreOp";
        // Flush hash table before the postOp
        pre_phase_.FlushAll();
        pre_phase_.CloseAll();

        if (use_post_thread_) {
            // waiting for the additional thread to finish the reduce
            thread_.join();
            // deallocate stream if already processed
            use_mix_stream_ ? mix_stream_.reset() : cat_stream_.reset();
        }
    }

    void Execute() final { }

    DIAMemUse PushDataMemUse() final {
        return DIAMemUse::Max();
    }

    void PushData(bool consume) final {
        if (!use_post_thread_ && !reduced_) {
            // not final reduced, and no additional thread, perform post reduce
            post_phase_.Initialize(DIABase::mem_limit_);
            ProcessChannel();

            // deallocate stream if already processed
            use_mix_stream_ ? mix_stream_.reset() : cat_stream_.reset();

            reduced_ = true;
        }
        post_phase_.PushData(consume);
    }

    //! process the inbound data in the post reduce phase
    void ProcessChannel() {
        if (use_mix_stream_)
        {
            auto reader = mix_stream_->GetMixReader(/* consume */ true);
            sLOG << "reading data from" << mix_stream_->id()
                 << "to push into post phase which flushes to" << this->dia_id();
            while (reader.HasNext()) {
                post_phase_.Insert(reader.template Next<TableItem>());
            }
        }
        else
        {
            auto reader = cat_stream_->GetCatReader(/* consume */ true);
            sLOG << "reading data from" << cat_stream_->id()
                 << "to push into post phase which flushes to" << this->dia_id();
            while (reader.HasNext()) {
                post_phase_.Insert(reader.template Next<TableItem>());
            }
        }
    }

    void Dispose() final {
        post_phase_.Dispose();
    }

private:
    // pointers for both Mix and CatStream. only one is used, the other costs
    // only a null pointer.
    data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;

    data::Stream::Writers emitters_;
    //! handle to additional thread for post phase
    std::thread thread_;

    core::ReducePrePhase<
        TableItem, Key, ValueType, KeyExtractor,
        ReduceFunction, VolatileKey, data::Stream::Writer, ReduceConfig,
        HashIndexFunction, KeyEqualFunction, KeyHashFunction,
        UseDuplicateDetection> pre_phase_;

    core::ReduceByHashPostPhase<
        TableItem, Key, ValueType, KeyExtractor, ReduceFunction, Emitter,
        VolatileKey, ReduceConfig,
        HashIndexFunction, KeyEqualFunction> post_phase_;

    bool reduced_ = false;
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReduceByKey(
    const KeyExtractor& key_extractor,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config) const {
    // forward to main function
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    return ReduceByKey(
        NoVolatileKeyTag, NoDuplicateDetectionTag,
        key_extractor, reduce_function, reduce_config,
        std::hash<Key>(), std::equal_to<Key>());
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig, typename KeyHashFunction>
auto DIA<ValueType, Stack>::ReduceByKey(
    const KeyExtractor& key_extractor,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function) const {
    // forward to main function
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    return ReduceByKey(
        NoVolatileKeyTag, NoDuplicateDetectionTag,
        key_extractor, reduce_function, reduce_config,
        key_hash_function, std::equal_to<Key>());
}

template <typename ValueType, typename Stack>
template <bool VolatileKeyValue,
          typename KeyExtractor, typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction, typename KeyEqualFunction>
auto DIA<ValueType, Stack>::ReduceByKey(
    const VolatileKeyFlag<VolatileKeyValue>& volatile_key_flag,
    const KeyExtractor& key_extractor,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function,
    const KeyEqualFunction& key_equal_funtion) const {
    // forward to main function
    return ReduceByKey(
        volatile_key_flag, NoDuplicateDetectionTag,
        key_extractor, reduce_function, reduce_config,
        key_hash_function, key_equal_funtion);
}

template <typename ValueType, typename Stack>
template <bool DuplicateDetectionValue,
          typename KeyExtractor, typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction, typename KeyEqualFunction>
auto DIA<ValueType, Stack>::ReduceByKey(
    const DuplicateDetectionFlag<DuplicateDetectionValue>& duplicate_detection_flag,
    const KeyExtractor& key_extractor,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function,
    const KeyEqualFunction& key_equal_funtion) const {
    // forward to main function
    return ReduceByKey(
        NoVolatileKeyTag, duplicate_detection_flag,
        key_extractor, reduce_function, reduce_config,
        key_hash_function, key_equal_funtion);
}

template <typename ValueType, typename Stack>
template <bool VolatileKeyValue,
          bool DuplicateDetectionValue,
          typename KeyExtractor, typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction, typename KeyEqualFunction>
auto DIA<ValueType, Stack>::ReduceByKey(
    const VolatileKeyFlag<VolatileKeyValue>&,
    const DuplicateDetectionFlag<DuplicateDetectionValue>&,
    const KeyExtractor& key_extractor,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function,
    const KeyEqualFunction& key_equal_funtion) const {
    assert(IsValid());

    using DOpResult
        = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::
                                template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    using ReduceNode = api::ReduceNode<
        DOpResult, KeyExtractor, ReduceFunction, ReduceConfig,
        KeyHashFunction, KeyEqualFunction,
        VolatileKeyValue, DuplicateDetectionValue>;

    auto node = tlx::make_counting<ReduceNode>(
        *this, "ReduceByKey",
        key_extractor, reduce_function, reduce_config,
        key_hash_function, key_equal_funtion);

    return DIA<DOpResult>(node);
}

/******************************************************************************/
// ReducePair

template <typename ValueType, typename Stack>
template <typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReducePair(
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config) const {
    // forward to main function
    using Key = typename ValueType::first_type;
    return ReducePair(reduce_function, reduce_config,
                      std::hash<Key>(), std::equal_to<Key>());
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction>
auto DIA<ValueType, Stack>::ReducePair(
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function) const {
    // forward to main function
    using Key = typename ValueType::first_type;
    return ReducePair(reduce_function, reduce_config,
                      key_hash_function, std::equal_to<Key>());
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction, typename KeyEqualFunction>
auto DIA<ValueType, Stack>::ReducePair(
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function,
    const KeyEqualFunction& key_equal_funtion) const {
    // forward to main function
    return ReducePair(NoDuplicateDetectionTag,
                      reduce_function, reduce_config,
                      key_hash_function, key_equal_funtion);
}

template <typename ValueType, typename Stack>
template <bool DuplicateDetectionValue,
          typename ReduceFunction, typename ReduceConfig,
          typename KeyHashFunction, typename KeyEqualFunction>
auto DIA<ValueType, Stack>::ReducePair(
    const DuplicateDetectionFlag<DuplicateDetectionValue>&,
    const ReduceFunction& reduce_function,
    const ReduceConfig& reduce_config,
    const KeyHashFunction& key_hash_function,
    const KeyEqualFunction& key_equal_funtion) const {
    assert(IsValid());

    using DOpResult
        = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(tlx::is_std_pair<ValueType>::value,
                  "ValueType is not a pair");

    static_assert(
        std::is_convertible<
            typename ValueType::second_type,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename ValueType::second_type,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            typename ValueType::second_type>::value,
        "ReduceFunction has the wrong output type");

    auto key_extractor = [](const ValueType& value) { return value.first; };

    auto reduce_pair_function =
        [reduce_function](const ValueType& a, const ValueType& b) {
            return ValueType(a.first, reduce_function(a.second, b.second));
        };

    using ReduceNode = api::ReduceNode<
        ValueType,
        decltype(key_extractor), decltype(reduce_pair_function),
        ReduceConfig, KeyHashFunction, KeyEqualFunction,
        /* VolatileKey */ false, DuplicateDetectionValue>;

    auto node = tlx::make_counting<ReduceNode>(
        *this, "ReducePair",
        key_extractor, reduce_pair_function, reduce_config,
        key_hash_function, key_equal_funtion);

    return DIA<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_BY_KEY_HEADER

/******************************************************************************/
