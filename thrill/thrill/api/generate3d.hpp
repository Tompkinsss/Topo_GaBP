/*******************************************************************************
 * thrill/api/generate.hpp
 *
 * DIANode for a generate operation. Performs the actual generate operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GENERATE_HEADER
#define THRILL_API_GENERATE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/ndarray.hpp>
#include <random>
#include <type_traits>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a Generate operation. Generate creates an DIA
 * according to a generator function. This function is used to generate a DIA of
 * a certain size by applying it to integers from 0 to size - 1.
 *
 * \tparam ValueType Output type of the Generate operation.
 * \tparam GenerateNode Type of the generate function.
 * \ingroup api_layer
 */
template <typename ValueType>
class GenerateNode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a GenerateNode. Sets the Context, parents, generator
     * function and file path.
     */
    GenerateNode(Context& ctx,std::vector<ValueType> values, size_t x_size, size_t y_size, size_t z_size)
        : Super(ctx, "Generate"),
          values_(values),
          x_size_(x_size),
          y_size_(y_size),
          z_size_(z_size)
    { }

    void PushData(bool /* consume */) final {
        common::Range local_x = context_.CalculateLocalRange3D(0,x_size_,y_size_,z_size_);
        common::Range local_y = context_.CalculateLocalRange3D(1,x_size_,y_size_,z_size_);
        common::Range local_z = context_.CalculateLocalRange3D(2,x_size_,y_size_,z_size_);

        for(size_t k = local_z.begin;k < local_z.end;k++){
            for (size_t i = local_x.begin; i < local_x.end; i++) {
                for(size_t j = local_y.begin;j < local_y.end;j++ ){
                    this->PushItem(common::getElement(values_,x_size_,y_size_,i,j,k));
                }
            }
        }
    }

private:
    std::vector<ValueType> values_;
    size_t x_size_;
    size_t y_size_;
    size_t z_size_;
};

/*!
 * Generate is a Source-DOp, which creates a DIA of given size using a
 * generator function. The generator function called for each index in the range
 * of `[0,size)` and must output exactly one item.
 *
 * \image html dia_ops/Generate.svg
 *
 * \param ctx Reference to the Context object
 *
 * \param size Size of the output DIA
 *
 * \param generate_function Generator function, which maps `size_t` from
 * `[0,size)` to elements. Input type has to be `size_t`.
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
auto Generate(Context& ctx, std::vector<ValueType> values, size_t x_size, size_t y_size, size_t z_size) {


    using GenerateNode =
        api::GenerateNode<ValueType>;

    auto node = tlx::make_counting<GenerateNode>(
        ctx, values, x_size, y_size, z_size);

    return DIA<ValueType>(node);
}

/*!
 * Generate is a Source-DOp, which creates a DIA of given size containing the
 * size_t indexes `[0,size)`.
 *
 * \image html dia_ops/Generate.svg
 *
 * \param ctx Reference to the Context object
 *
 * \param size Size of the output DIA
 *
 * \ingroup dia_sources
 */

} // namespace api

//! imported from api namespace
using api::Generate;

} // namespace thrill

#endif // !THRILL_API_GENERATE_HEADER

/******************************************************************************/
