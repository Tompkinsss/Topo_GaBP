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
    GenerateNode(Context& ctx,std::vector<ValueType> values)
        : Super(ctx, "Generate"),
          values_(values)
    { }

    void PushData(bool /* consume */) final {
        common::Range local = context_.CalculateLocalRange(values_.size());

        for (size_t i = local.begin; i < local.end; i++) {
            this->PushItem(values_[i]);
            std::cout << values_[i] << " ";
        }
        std::cout << std::endl;
    }

private:
    std::vector<ValueType> values_;
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
auto Generate(Context& ctx, std::vector<ValueType> values) {


    using GenerateNode =
        api::GenerateNode<ValueType>;

    auto node = tlx::make_counting<GenerateNode>(
        ctx, values);

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
