/*******************************************************************************
 * thrill/api/zip_with_index.hpp
 *
 * DIANode for a ZipWithIndex operation.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ZIP_WITH_INDEX_HEADER
#define THRILL_API_ZIP_WITH_INDEX_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <functional>

namespace thrill {
namespace api {

/*!
 * A DIANode which calculates the array index for each items and performs a
 * Zip-like operation without extra rebalancing of the DIA data. This DIANode
 * supports only one parent, if more than one inputs must be zipped, use the
 * general Zip() with a Generate() DIA.
 *
 * \ingroup api_layer
 */
template <typename ValueType, typename ZipFunction>
class ZipWithIndexNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using InputType =
        typename common::FunctionTraits<ZipFunction>::template arg_plain<0>;

public:
    /*!
     * Constructor for a ZipNode.
     */
    template <typename ParentDIA>
    ZipWithIndexNode(
        const ZipFunction& zip_function, const ParentDIA& parent)
        : Super(parent.ctx(), "ZipWithIndex",
                { parent.id() }, { parent.node() }),
          zip_function_(zip_function),
          parent_stack_empty_(ParentDIA::stack_empty) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const InputType& input) {
                             writer_.Put(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "ZipWithIndex rejected File from parent due to Stack";
            return false;
        }

        // accept file
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        return true;
    }

    void StopPreOp(size_t /* parent_index */) final {
        writer_.Close();
    }

    void Execute() final {
        //! number of elements of this worker
        size_t dia_local_size = file_.num_items();
        sLOG << "dia_local_size" << dia_local_size;

        dia_local_rank_ = context_.net.ExPrefixSum(dia_local_size);
        sLOG << "dia_local_rank_" << dia_local_rank_;
    }

    void PushData(bool consume) final {
        size_t result_count = file_.num_items();

        data::File::Reader reader = file_.GetReader(consume);
        size_t index = dia_local_rank_;

        while (reader.HasNext()) {
            this->PushItem(
                zip_function_(reader.template Next<InputType>(), index++));
        }

        if (debug) {
            context_.PrintCollectiveMeanStdev(
                "Zip() result_count", result_count);
        }
    }

    void Dispose() final {
        file_.Clear();
    }

private:
    //! Zip function
    ZipFunction zip_function_;

    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

    //! File for intermediate storage
    data::File file_ { context_.GetFile(this) };

    //! Writer to intermediate file
    data::File::Writer writer_ { file_.GetWriter() };

    //! \name Variables for Calculating Global Index
    //! \{

    //! exclusive prefix sum over the number of items in workers
    size_t dia_local_rank_;

    //! \}
};

template <typename ValueType, typename Stack>
template <typename ZipFunction>
auto DIA<ValueType, Stack>::ZipWithIndex(
    const ZipFunction& zip_function) const {

    static_assert(
        common::FunctionTraits<ZipFunction>::arity == 2,
        "ZipWithIndexFunction must take exactly two parameters");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ZipFunction>::template arg<0>
            >::value,
        "ZipWithIndexFunction has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            size_t,
            typename common::FunctionTraits<ZipFunction>::template arg<1>
            >::value,
        "ZipWithIndexFunction must take a const unsigned long int& (aka. size_t)"
        " as second parameter");

    using ZipResult
        = typename common::FunctionTraits<ZipFunction>::result_type;

    using ZipWithIndexNode = api::ZipWithIndexNode<ZipResult, ZipFunction>;

    auto node = tlx::make_counting<ZipWithIndexNode>(zip_function, *this);

    return DIA<ZipResult>(node);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ZIP_WITH_INDEX_HEADER

/******************************************************************************/
