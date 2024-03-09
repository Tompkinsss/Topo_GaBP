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
class InterMap3DNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
 
    template <typename ParentDIA>
    explicit InterMap3DNode(const ParentDIA& parent, const InterMapFunction& inter_map_function,size_t table_element_num, size_t up_tables, size_t down_tables)
        : Super(parent.ctx(), "InterMap3D", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        table_element_num_(table_element_num),
        up_tables_(up_tables),
        down_tables_(down_tables),
        cat_stream_(parent.ctx().GetNewCatStream(this))
        ,emitters_(cat_stream_->GetWriters()) 
        {
        auto pre_op_fn = [this](const ValueType& input) {
                           PreOp(input);
                       };
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
        my_rank_ = this->context().my_rank();
        total_rank_ = this->context().num_workers();
    }

    void PreOp(const ValueType& input) {
        values_.push_back(input);
    }

    void StartPreOp(size_t parent_index) final {
    }

    void StopPreOp(size_t parent_index) final {
        size_t size = values_.size();
        size_t up_num = table_element_num_ * up_tables_;
        size_t down_num = table_element_num_ * down_tables_;
        if(up_num > 0){
            up_values_ = context_.net.Predecessor(up_num,values_);
        }
        if(down_num > 0){
            down_values_ = context_.net.Successor(down_num,values_);
        }

        if(up_values_.size() > 0){
            values_.insert(values_.begin(),up_values_.begin(),up_values_.end());
        } 
        if(down_values_.size() > 0){
            values_.insert(values_.end(),down_values_.begin(),down_values_.end());
        }
    }

    //! Executes the rebalance operation.
    void Execute() final {


    }

    void ProcessChannel(){

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
    std::vector<ValueType> up_values_;
    std::vector<ValueType> down_values_;

    size_t my_rank_;
    size_t total_rank_;

    size_t table_element_num_; 
    size_t up_tables_;
    size_t down_tables_;

    //data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;
    data::Stream::Writers emitters_;
    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap3D(const InterMapFunction& inter_map_function, size_t table_element_num, size_t up_tables, size_t down_tables) const {
    using InterMap3DNode = api::InterMap3DNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMap3DNode>(*this, inter_map_function, table_element_num, up_tables, down_tables));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/
