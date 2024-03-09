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
#include <math.h>

namespace thrill {
namespace api {
    
/*!
 * \ingroup api_layer
 */
template <typename ValueType,typename InterMapFunction>
class InterMapNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
 
    template <typename ParentDIA>
    explicit InterMapNode(const ParentDIA& parent, const InterMapFunction& inter_map_function,size_t line_element_num,size_t neighber_rows)
        : Super(parent.ctx(), "InterMap", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        line_element_num_(line_element_num),
        neighber_rows_(neighber_rows),
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
        std::cout <<"pre op " << std::endl;
        values_.push_back(input);
    }

    void StartPreOp(size_t parent_index) final {
    }

    void StopPreOp(size_t parent_index) final {
        size_t size = values_.size();
        size_t num = line_element_num_ * neighber_rows_;
        if(num > size)
            num = size;

       std::cout << "send values to neighber worker (before)" << std::endl;
       up_values_ = context_.net.Predecessor(num, values_);
       down_values_ = context_.net.Successor(num, values_);

       std::cout << "send values to neighber worker" << std::endl;

    }

    //! Executes the rebalance operation.
    void Execute() final {


    }

    void ProcessChannel(){

        //int i;
        //values_.insert(values_.begin(),up_values_.begin(),up_values_.end());
        //values_.insert(values_.end(),down_values_.begin(),down_values_.end());

    }


    void PushData(bool consume) final {


        //ProcessChannel();

        std::cout << "push data." << std::endl;

        std::vector<ValueType> result; 

        int i;
        for(i=0;i<values_.size();i++){
            ValueType left_value,right_value,up_value,down_value;

            if(i%line_element_num_==0){
                left_value = values_[i]-values_[i];
            }else{
                left_value = values_[i-1];
            }

            if(i%line_element_num_ == line_element_num_-1){
                right_value = values_[i]-values_[i];
            }else{
                right_value = values_[i+1];
            }
 
            if(i < line_element_num_){
                if(up_values_.size()>0){
                    up_value = up_values_[up_values_.size()-line_element_num_ + i];
                }else{
                    up_value = values_[i] - values_[i];
                }
            }else{
                up_value = values_[i - line_element_num_];
            }

            if(i > values_.size() - line_element_num_){

                if(down_values_.size() > 0){
                    down_value = down_values_[i % line_element_num_];
                }else{
                    down_value = values_[i] - values_[i];
                }
            }else{
                down_value = values_[i + line_element_num_];
            }

            ValueType value = inter_map_function_(values_[i],left_value,right_value,up_value,down_value);
            result.push_back(value);

        }

        

        typename std::vector<ValueType>::iterator itr = result.begin();

        for(; itr!=result.end();++itr)
        { 
            this->PushItem(*itr);
        }

        std::cout << "push data end." << std::endl;
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

    size_t line_element_num_;
    size_t neighber_rows_;
    //data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;
    data::Stream::Writers emitters_;
    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap(const InterMapFunction& inter_map_function, size_t line_element_num, size_t neighber_rows) const {
    using InterMapNode = api::InterMapNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMapNode>(*this, inter_map_function, line_element_num, neighber_rows));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/
