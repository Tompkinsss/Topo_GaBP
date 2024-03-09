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
class InterMap2DNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
 
    template <typename ParentDIA>
    explicit InterMap2DNode(const ParentDIA& parent, const InterMapFunction& inter_map_function,size_t line_element_num, size_t up_lines, size_t down_lines)
        : Super(parent.ctx(), "InterMap2D", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        line_element_num_(line_element_num),
        up_lines_(up_lines),
        down_lines_(down_lines)
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
        size_t up_num = line_element_num_ * up_lines_;
        size_t down_num = line_element_num_ * down_lines_;
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


	    values_.clear();
	    values_.shrink_to_fit();
	    up_values_.clear();
	    up_values_.shrink_to_fit();
	    down_values_.clear();
	    down_values_.shrink_to_fit();

    }

private:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

    std::vector<ValueType> values_;
    std::vector<ValueType> up_values_;
    std::vector<ValueType> down_values_;

    size_t my_rank_;
    size_t total_rank_;

    size_t line_element_num_; 
    size_t up_lines_;
    size_t down_lines_;

    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap2D(const InterMapFunction& inter_map_function, size_t line_element_num, size_t up_lines, size_t down_lines) const {
    using InterMap2DNode = api::InterMap2DNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMap2DNode>(*this, inter_map_function, line_element_num, up_lines, down_lines));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/
