/*******************************************************************************
 * thrill/api/rebalance.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_INTERMAP_HEADER
#define THRILL_API_INTERMAP_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/block_writer.hpp>

#include <algorithm>
#include <vector>

namespace thrill {
namespace api {
    
/*!
 * \ingroup api_layer
 */
template <typename ValueType,typename InterMapFunction>
class InterMap1DNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
 
    template <typename ParentDIA>
    explicit InterMap1DNode(const ParentDIA& parent, const InterMapFunction& inter_map_function,size_t left_neighber_count,size_t right_neighber_count)
        : Super(parent.ctx(), "InterMap1D", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        left_neighber_count_(left_neighber_count),
        right_neighber_count_(right_neighber_count),
        cat_stream_(parent.ctx().GetNewCatStream(this))
        ,emitters_(cat_stream_->GetWriters()) 
        {
        auto pre_op_fn = [this](const ValueType& input) {
                           PreOp(input);
                       };
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
        my_rank_ = this->context().my_rank();
        total_rank_ = this->context().num_hosts() * this->context().workers_per_host();

    }

    void PreOp(const ValueType& input) {
        values_.push_back(input);
    }

    void StartPreOp(size_t parent_index) final {
    }

    void StopPreOp(size_t parent_index) final {
        size_t size = values_.size();
        if(left_neighber_count_ > size)
            left_neighber_count_ = size;
        if(right_neighber_count_ > size)
            right_neighber_count_ = size;

       left_values_ = context_.net.Predecessor(left_neighber_count_, values_);
       right_values_ = context_.net.Successor(right_neighber_count_, values_);

    }

    //! Executes the rebalance operation.
    void Execute() final {


    }

    void ProcessChannel(){
        int i;
        values_.insert(values_.begin(),left_values_.begin(),left_values_.end());
        values_.insert(values_.end(),right_values_.begin(),right_values_.end());

    }


    void PushData(bool consume) final {

        ProcessChannel();

        std::vector<ValueType> result = inter_map_function_(values_);

 
        typename std::vector<ValueType>::iterator itr = result.begin();

        for(; itr!=result.end();++itr)
        { 
            this->PushItem(*itr);
        }
    }

    void Dispose() final {
    }

private:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

    std::vector<ValueType> values_;
    std::vector<ValueType> left_values_;
    std::vector<ValueType> right_values_;
 
    size_t my_rank_;
    size_t total_rank_;

    size_t left_neighber_count_;
    size_t right_neighber_count_;

    //data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;
    data::Stream::Writers emitters_;
    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap1D(const InterMapFunction& inter_map_function, size_t left_neighber_count, size_t right_neighber_count) const {
    using InterMap1DNode = api::InterMap1DNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMap1DNode>(*this, inter_map_function, left_neighber_count, right_neighber_count));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/