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
    explicit InterMap2DNode(const ParentDIA& parent, const InterMapFunction& inter_map_function, size_t rows, size_t columns, size_t left_size, size_t right_size, size_t up_size, size_t down_size)
        : Super(parent.ctx(), "InterMap2D", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        rows_(rows),
        columns_(columns),
        left_size_(left_size),
        right_size_(right_size),
        up_size_(up_size),
        down_size_(down_size),
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

        size_t sub_rank = (int)sqrt(total_rank_);

        size_t x_rank = my_rank_ / sub_rank;
        size_t y_rank = my_rank_ % sub_rank;

        size_t sub_rows = rows_ / sub_rank;
        size_t sub_columns = columns_ / sub_rank;
 

        if(x_rank > 0){
            for(size_t i = 0; i < down_size_ * sub_columns; i++){
                std::pair<ValueType,int> item(values_[i],3);
                emitters_[(x_rank-1)*sub_rank+y_rank].Put(item);
            }            
 
        }
        if(x_rank < sub_rank - 1){
            for(size_t i = 0; i < up_size_ * sub_columns; i++){
                
                std::pair<ValueType,int> item(values_[values_.size() - up_size_ * sub_columns + i],2);
                emitters_[(x_rank + 1) * sub_rank + y_rank].Put(item);
            }

        }
        if(y_rank > 0){
            for(size_t i = 0; i < right_size_; i++){
                for(size_t j = 0; j < sub_rows; j++){
                    std::pair<ValueType,int> item(values_[j * sub_columns + i],1);
                    emitters_[x_rank * sub_rank + y_rank - 1].Put(item);
                }
            }
        }
        if(y_rank < sub_rank - 1){
            for(size_t i = 0; i < left_size_; i++)
                for(size_t j = 0; j< sub_rows; j++){

                    std::pair<ValueType,int> item(values_[(j + 1) * sub_columns - i - 1],0);
                    emitters_[x_rank * sub_rank + y_rank + 1].Put(item);
            }
        }


        //emitters_.Close();
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Flush();
            emitters_[i].Close();
        } 

        context_.net.Barrier();
        

    }

    //! Executes the rebalance operation.
    void Execute() final {


    }

    void ProcessChannel(){

    auto reader = cat_stream_ -> GetCatReader(true);

        while(reader.HasNext()){
            std::pair<ValueType,int> item = reader.template Next<std::pair<ValueType,int> >();
            if(item.second == 0){
                left_values_.push_back(item.first);
            }else if(item.second == 1){
                right_values_.push_back(item.first);
            }else if(item.second == 2){
                up_values_.push_back(item.first);
            }else if(item.second == 3){
                down_values_.push_back(item.first);
            }
        }
    }


    void PushData(bool consume) final {

        ProcessChannel();

        std::vector<ValueType> results;

        size_t sub_columns = columns_ / (int)sqrt(total_rank_);
        size_t sub_rows = rows_ / (int)sqrt(total_rank_);
        
        for(size_t i=0;i<values_.size();i++){

            std::vector<ValueType> left_neighbers;
            if(left_values_.size() > 0 && i % sub_columns < left_size_){
                for(size_t j = i % sub_columns; j < left_size_; j++){
                    left_neighbers.push_back(left_values_[i / sub_columns * left_size_ + j]);
                }
            }
            if(i % sub_columns > 0){
                for(size_t j = 0; j < left_size_ && j < i % sub_columns; j++){
                    left_neighbers.push_back(values_[i - left_size_ + j]);
                }
            }

            std::vector<ValueType> right_neighbers;
            if(right_values_.size() > 0 && sub_columns - 1 - (i % sub_columns) < right_size_){
                for(size_t j = sub_columns - 1 - (i % sub_columns); j < right_size_; j++){
                    right_neighbers.push_back(right_values_[i / sub_columns * right_size_ + j]);
                }
            }
            if(sub_columns - 1 - (i % sub_columns) > 0){
                for(size_t j = 0; j < right_size_ && j < sub_columns - 1 - (i % sub_columns) ; j++){
                    right_neighbers.push_back(values_[i + right_size_ - j]);
                }
            }

            std::vector<ValueType> up_neighbers;
            if(up_values_.size() > 0 && i / sub_columns < up_size_){
                for(size_t j = i / sub_columns; j < up_size_; j++){
                    up_neighbers.push_back(up_values_[j * sub_columns + i % sub_columns]);
                }
            }
            if(i / sub_columns > 0){
                for(size_t j = 0; j < up_size_ && j < i / sub_columns; j++){
                    up_neighbers.push_back(values_[i - (j+1) * sub_columns]);
                }
            }

            std::vector<ValueType> down_neighbers;

 
            if(down_values_.size() > 0 && sub_rows - i / sub_columns <= down_size_){
                for(size_t j = sub_rows - i / sub_columns - 1;j < down_size_; j++){
                    down_neighbers.push_back(down_values_[j * sub_columns + i % sub_columns]);
                }
            }
            if(sub_rows - i / sub_columns > 1){
                for(size_t j = 0; j < down_size_ && j < sub_rows - i / sub_columns; j++){
                    down_neighbers.push_back(values_[i + (j+1) * sub_columns]);
                }
            }
            ValueType result = inter_map_function_(values_[i],left_neighbers, right_neighbers, up_neighbers, down_neighbers);
            results.push_back(result);
        }
        
        typename std::vector<ValueType>::iterator itr = results.begin();

        for(; itr!=results.end();++itr)
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
    std::vector<ValueType> left_values_;
    std::vector<ValueType> right_values_;

    size_t my_rank_;
    size_t total_rank_;

    size_t rows_;
    size_t columns_; 
    size_t up_size_;
    size_t down_size_;
    size_t left_size_;
    size_t right_size_;

    //data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;
    data::CatStream::Writers emitters_;
    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap2D(const InterMapFunction& inter_map_function, size_t rows, size_t columns, size_t left_size, size_t right_size, size_t up_size, size_t down_size) const {
    using InterMap2DNode = api::InterMap2DNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMap2DNode>(*this, inter_map_function, rows, columns, left_size, right_size, up_size, down_size));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/
