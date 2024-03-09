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
#ifndef THRILL_API_INTERMAP_3D_HEADER
#define THRILL_API_INTERMAP_3D_HEADER

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
    explicit InterMap3DNode(const ParentDIA& parent, const InterMapFunction& inter_map_function, size_t rows, size_t columns, size_t layers, size_t left_size, size_t right_size, size_t up_size, size_t down_size, size_t front_size, size_t back_size)
        : Super(parent.ctx(), "InterMap3D", { parent.id() }, { parent.node() })
        ,parent_stack_empty_(ParentDIA::stack_empty),
        inter_map_function_(inter_map_function),
        rows_(rows),
        columns_(columns),
        layers_(layers),
        left_size_(left_size),
        right_size_(right_size),
        up_size_(up_size),
        down_size_(down_size),
        front_size_(front_size),
        back_size_(back_size),
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

        size_t sub_rank = (int)pow(total_rank_, 1.0/3);
        std::cout << "sub_rank = " << sub_rank << std::endl;

        size_t x_rank = (my_rank_ % (sub_rank * sub_rank)) / sub_rank;
        size_t y_rank = (my_rank_ % (sub_rank * sub_rank)) % sub_rank;
        size_t z_rank = my_rank_ / (sub_rank * sub_rank); 

        size_t sub_rows = rows_ / sub_rank;
        size_t sub_columns = columns_ / sub_rank;
        size_t sub_layers = layers_ / sub_rank; 

        std::cout << "sub_rows=" << sub_rows << " sub_columns=" << sub_columns << " sub_layers=" << sub_layers << std::endl;

        std::cout << "left_size=" << left_size_ << " right_size=" << right_size_ << " up_size=" << up_size_ << " down_size=" << down_size_ << " front_size=" << front_size_ << " back_size=" << back_size_ << std::endl;

        if(x_rank > 0){
            for(size_t j = 0; j < sub_layers; j++){
            for(size_t i = 0; i < down_size_ * sub_columns; i++){
                std::pair<ValueType,int> item(values_[j * sub_rows * sub_columns + i],3);
                emitters_[z_rank * sub_rank * sub_rank + (x_rank-1) * sub_rank + y_rank].Put(item);
            }
            }            
 
        }
        if(x_rank < sub_rank - 1){
            for(size_t j = 0; j < sub_layers; j++){
            for(size_t i = 0; i < up_size_ * sub_columns; i++){
                std::pair<ValueType,int> item(values_[sub_rows * sub_columns * (j + 1) - up_size_ * sub_columns + i],2);
                emitters_[z_rank * sub_rank * sub_rank + (x_rank + 1) * sub_rank + y_rank].Put(item);
                }
            }

        }
        if(y_rank > 0){
            for(size_t k = 0; k < sub_layers;k++){
            for(size_t i = 0; i < right_size_; i++){
                for(size_t j = 0; j < sub_rows; j++){
                    std::pair<ValueType,int> item(values_[k * sub_rows * sub_columns + j * sub_columns + i],1);
                    emitters_[z_rank * sub_rank * sub_rank + x_rank * sub_rank + y_rank - 1].Put(item);
                }
            }
            }
        }
        if(y_rank < sub_rank - 1){
            for(size_t k = 0; k < sub_layers; k++){
            for(size_t i = 0; i < left_size_; i++){
                for(size_t j = 0; j< sub_rows; j++){

                    std::pair<ValueType,int> item(values_[k * sub_rows * sub_columns + (j + 1) * sub_columns - i - 1],0);
                    emitters_[z_rank * sub_rank * sub_rank + x_rank * sub_rank + y_rank + 1].Put(item);
                }
            }
            }
        }
        if(z_rank > 0){
            for(size_t i = 0; i < front_size_; i++){
                for(size_t j = 0; j < sub_rows; j++){
                    for(size_t k = 0; k < sub_columns; k++){
                        std::pair<ValueType,int> item(values_[i * sub_rows * sub_columns + j * sub_columns + k],4);
                        emitters_[(z_rank - 1) * sub_rank * sub_rank + x_rank * sub_rank + y_rank].Put(item);
                    }
                }
 
            }
        }

        if(z_rank < sub_rank -1){
            for(size_t i=0; i < back_size_; i++){
                for(size_t j = 0; j < sub_rows; j++){
                    for(size_t k = 0; k < sub_columns; k++){
                        std::pair<ValueType,int> item(values_[(sub_layers - i - 1) * sub_rows * sub_columns + j * sub_columns + k],5);
                        emitters_[(z_rank + 1) * sub_rank * sub_rank + x_rank * sub_rank + y_rank].Put(item);
                    }
                }
            }
        }

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
            }else if(item.second == 4){
                front_values_.push_back(item.first);
            }else if(item.second == 5){
                back_values_.push_back(item.first);
            }
        }

    }


    void PushData(bool consume) final {

        ProcessChannel();

        std::vector<ValueType> results;

        size_t sub_rank = (int)pow(total_rank_, 1.0/3);
        size_t sub_rows = rows_ / sub_rank;
        size_t sub_columns = columns_ / sub_rank;
        size_t sub_layers = layers_ / sub_rank;
 
        for(size_t i=0;i<values_.size();i++){

            std::vector<ValueType> left_neighbers;
            if(i % sub_columns > 0){
                for(size_t j = 0; j < left_size_ && j < i % sub_columns; j++){
                    left_neighbers.push_back(values_[i - j - 1]);
                }
            }
            if(left_values_.size() > 0 && i % sub_columns  < left_size_){
                for(size_t j = 0; j < left_size_ - i % sub_columns; j++){
                    left_neighbers.push_back(left_values_[(i / (sub_rows * sub_columns)) * left_size_ * sub_rows + (i % (sub_columns * sub_rows)) / sub_columns + j * sub_rows]);
                }
            }
           
            std::vector<ValueType> right_neighbers;
            if(sub_columns - 1 - (i % sub_columns) > 0){
                for(size_t j = 0; j < right_size_ && j < sub_columns - 1 - (i % sub_columns) ; j++){
                    right_neighbers.push_back(values_[i + j + 1]);
                }
            }
            if(right_values_.size() > 0 && sub_columns - 1 - (i % sub_columns) < right_size_){
                for(size_t j = 0; j < right_size_-(sub_columns - 1 - (i % sub_columns)); j++){
                    right_neighbers.push_back(right_values_[(i / (sub_rows * sub_columns)) * right_size_ * sub_rows + (i % (sub_columns * sub_rows))/ sub_columns + j * sub_rows]);
                }
            }

            std::vector<ValueType> up_neighbers;
            if((i % (sub_columns * sub_rows))/ sub_columns > 0){
                for(size_t j = 0; j < up_size_ && j < (i % (sub_columns * sub_rows)) / sub_columns; j++){
                    up_neighbers.push_back(values_[i - (j+1) * sub_columns]);
                }
            }

            if(up_values_.size() > 0 && (i % (sub_columns * sub_rows))/ sub_columns < up_size_){
                for(size_t j = (i % (sub_columns * sub_rows))/ sub_columns; j < up_size_; j++){
                    up_neighbers.push_back(up_values_[i / (sub_rows * sub_columns) * up_size_ * sub_columns + (i % sub_columns) + j * sub_columns]);
                }
            }

            std::vector<ValueType> down_neighbers;
            if(sub_rows - (i % (sub_columns * sub_rows)) / sub_columns > 1){
                for(size_t j = 0; j < down_size_ && j < sub_rows - (i % (sub_columns * sub_rows)) / sub_columns - 1; j++){
                    down_neighbers.push_back(values_[i + (j+1) * sub_columns]);
                }
            }
            if(down_values_.size() > 0 && sub_rows - (i % (sub_columns * sub_rows)) / sub_columns <= down_size_){
                for(size_t j = 0; j < down_size_ - (sub_rows - (i % (sub_columns * sub_rows)) / sub_columns - 1); j++){
                    down_neighbers.push_back(down_values_[i / (sub_rows * sub_columns) * down_size_ * sub_columns + (i % sub_columns) + j * sub_columns]);
                }
            }
            
            std::vector<ValueType> front_neighbers;
            if(sub_layers - i / (sub_columns * sub_rows) > 1){
                for(size_t j = 0; j < front_size_ && j < sub_layers - i / (sub_columns * sub_rows) - 1; j++){
                    front_neighbers.push_back(values_[i + (j + 1) * sub_columns * sub_rows]);
                }
            }
            if(front_values_.size() > 0 && sub_layers - i / (sub_rows * sub_columns) - 1 < front_size_){
                for(size_t j = 0; j < front_size_ - (sub_layers - i / (sub_rows * sub_columns) - 1); j++){
                    front_neighbers.push_back(front_values_[j * sub_columns * sub_rows + i % (sub_columns * sub_rows)]);
                }
            }
            
            std::vector<ValueType> back_neighbers;
            if(i / (sub_columns * sub_rows) > 0){
                for(size_t j = 0; j < back_size_ && j < i / (sub_columns * sub_rows); j++){
                    back_neighbers.push_back(values_[i - (j+1) * sub_columns * sub_rows]);
                }
            }
            if(back_values_.size() > 0 && i / (sub_rows * sub_columns) < back_size_){
                for(size_t j = 0; j < back_size_ - (i / (sub_columns * sub_rows)); j++){
                    back_neighbers.push_back(back_values_[j * sub_columns * sub_rows + i % (sub_columns * sub_rows)]);
                }
            }
            
            std::cout << "value:" << values_[i] << " left_neighbers:";
            for(size_t y = 0;y < left_neighbers.size();y++){
                std::cout << left_neighbers[y] << " ";
            }
            std::cout << "right_neighbers:";
            for(size_t y = 0;y < right_neighbers.size();y++){
                std::cout << right_neighbers[y] << " ";
            }
            std::cout << "up_neighbers:";
            for(size_t y = 0;y < up_neighbers.size();y++){
                std::cout << up_neighbers[y] << " ";
            }
             std::cout << "down_neighbers:";
            for(size_t y = 0;y < down_neighbers.size();y++){
                std::cout << down_neighbers[y] << " ";
            }
             std::cout << "front_neighbers:";
            for(size_t y = 0;y < front_neighbers.size();y++){
                std::cout << front_neighbers[y] << " ";
            }
              std::cout << "back_neighbers:";
            for(size_t y = 0;y < back_neighbers.size();y++){
                std::cout << back_neighbers[y] << " ";
            }
                std::cout << std::endl;
 
            ValueType result = inter_map_function_(values_[i],left_neighbers, right_neighbers, up_neighbers, down_neighbers, front_neighbers, back_neighbers);
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
    std::vector<ValueType> front_values_;
    std::vector<ValueType> back_values_;

    size_t my_rank_;
    size_t total_rank_;

    size_t rows_;
    size_t columns_;
    size_t layers_; 
    size_t up_size_;
    size_t down_size_;
    size_t left_size_;
    size_t right_size_;
    size_t front_size_;
    size_t back_size_;

    //data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;
    data::CatStream::Writers emitters_;
    InterMapFunction inter_map_function_;
};

template <typename ValueType, typename Stack>
template <typename InterMapFunction>
auto DIA<ValueType, Stack>::InterMap3D(const InterMapFunction& inter_map_function, size_t rows, size_t columns, size_t layers, size_t left_size, size_t right_size, size_t up_size, size_t down_size, size_t front_size, size_t back_size) const {
    using InterMap3DNode = api::InterMap3DNode<ValueType,InterMapFunction>;
    return DIA<ValueType>(tlx::make_counting<InterMap3DNode>(*this, inter_map_function, rows, columns, layers, left_size, right_size, up_size, down_size, front_size, back_size));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_INTERMAP_HEADER

/******************************************************************************/
