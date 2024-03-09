/*******************************************************************************
 * thrill/api/write_lines_one.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_LINES_ONE_HEADER
#define THRILL_API_WRITE_LINES_ONE_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/data/file.hpp>

#include <fstream>
#include <string>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class WriteLinesOneNode final : public ActionNode
{
    static constexpr bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    template <typename ParentDIA>
    WriteLinesOneNode(const ParentDIA& parent,
                      const std::string& path_out)
        : ActionNode(parent.ctx(), "WriteLinesOne",
                     { parent.id() }, { parent.node() }),
          path_out_(path_out),
          file_(path_out_, std::ios::binary) {
        sLOG << "Creating write node.";

        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void PreOp(const ValueType& input) {
        writer_.Put(input);
        local_size_ += input.size() + 1;
        local_lines_++;
    }

    void StopPreOp(size_t /* parent_index */) final {
        writer_.Close();
    }

    //! Closes the output file
    void Execute() final {
        Super::logger_
            << "class" << "WriteLinesOneNode"
            << "total_bytes" << local_size_
            << "total_lines" << local_lines_;

        // (Portable) allocation of output file, setting individual file pointers.
        size_t prefix_elem = context_.net.ExPrefixSum(local_size_);
        if (context_.my_rank() == context_.num_workers() - 1) {
            file_.seekp(prefix_elem + local_size_ - 1);
            file_.put('\0');
        }
        file_.seekp(prefix_elem);
        context_.net.Barrier();

        data::File::ConsumeReader reader = temp_file_.GetConsumeReader();
        size_t num_items = temp_file_.num_items();

        for (size_t i = 0; i < num_items; ++i) {
            file_ << reader.Next<ValueType>() << '\n';
        }
    }

private:
    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;

    //! Local file size
    size_t local_size_ = 0;

    //! Temporary File for splitting correctly?
    data::File temp_file_ { context_.GetFile(this) };

    //! File writer used.
    data::File::Writer writer_ { temp_file_.GetWriter() };

    size_t local_lines_ = 0;
};

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::WriteLinesOne(
    const std::string& filepath) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLinesOne needs an std::string as input parameter");

    using WriteLinesOneNode = api::WriteLinesOneNode<ValueType>;

    auto node = tlx::make_counting<WriteLinesOneNode>(*this, filepath);

    node->RunScope();
}

template <typename ValueType, typename Stack>
Future<void> DIA<ValueType, Stack>::WriteLinesOneFuture(
    const std::string& filepath) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLinesOne needs an std::string as input parameter");

    using WriteLinesOneNode = api::WriteLinesOneNode<ValueType>;

    auto node = tlx::make_counting<WriteLinesOneNode>(*this, filepath);

    return Future<void>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_ONE_HEADER

/******************************************************************************/
