/*******************************************************************************
 * thrill/api/dia_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_NODE_HEADER
#define THRILL_API_DIA_NODE_HEADER

#include <thrill/api/dia_base.hpp>
#include <thrill/data/file.hpp>
#include <tlx/delegate.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

/*!
 * A DIANode is a typed node representing and operation in Thrill. It is the
 * super class for all operation nodes and stores the state of the
 * operation.
 *
 * \tparam ValueType The output type of the DIA delivered by this DIANode.
 */
template <typename ValueType>
class DIANode : public DIABase
{
public:
    using Callback = tlx::delegate<void (const ValueType&)>;

    struct Child {
        //! reference to child node
        DIABase  * node;
        //! callback to invoke (currently for each item)
        Callback callback;
        //! index this node has among the parents of the child (passed to
        //! callbacks), e.g. for ZipNode which has multiple parents and their order
        //! is important.
        size_t   parent_index;
    };

    /*!
     * Constructor for a DIANode, which sets references to the
     * parent nodes. Calls the constructor of DIABase with the same parameters.
     */
    DIANode(Context& ctx, const char* label,
            const std::initializer_list<size_t>& parent_ids,
            const std::initializer_list<DIABasePtr>& parents)
        : DIABase(ctx, label, parent_ids, parents) {
       

	}

    /*!
     * Constructor for a DIANode, which sets references to the
     * parent nodes. Calls the constructor of DIABase with the same parameters.
     */
    DIANode(Context& ctx, const char* label,
            std::vector<size_t>&& parent_ids,
            std::vector<DIABasePtr>&& parents)
        : DIABase(ctx, label, std::move(parent_ids), std::move(parents)) { }

    /*!
     * Enables children to push their "folded" function chains to their parent.
     * This way the parent can push all its result elements to each of the
     * children. This procedure enables the minimization of IO-accesses.
     */
    virtual void AddChild(DIABase* node, const Callback& callback = Callback(),
                          size_t parent_index = 0) {
        children_.emplace_back(Child { node, callback, parent_index }
			);
	

    }

    //! Remove a child from the vector of children. This method is called by the
    //! destructor of children.
    void RemoveChild(DIABase* node) override {
        typename std::vector<Child>::iterator swap_end =
            std::remove_if(
                children_.begin(), children_.end(),
                [node](const Child& c) { return c.node == node; });

        // assert(swap_end != children_.end());
        children_.erase(swap_end, children_.end());
    }

    void RemoveAllChildren() override {
        // remove all children other than Collapse and Union nodes
        children_.erase(
            std::remove_if(
                children_.begin(), children_.end(),
                [this](Child& child) {
                    if (child.node->ForwardDataOnly())
                        return false;
                    child.node->RemoveParent(this);
                    return true;
                }),
            children_.end());

        // recurse into remaining nodes (CollapseNode)
        for (Child& child : children_)
            child.node->RemoveAllChildren();
    }

    //! Returns the children of this DIABase.
    void children(std::vector<DIABase*>& out) const override {
	    int size = children_.size();
        out.reserve(children_.size());
        for (const Child& child : children_){
            out.emplace_back(child.node);
	}
        return;
    }

    //! Performing push operation. Notifies children and calls actual push
    //! method. Then cleans up the DIA graph by freeing parent references of
    //! children.
    void RunPushData() override {
        bool need_callback = false;
        for (const Child& child : children_)
            need_callback = need_callback || (child.callback != nullptr);
        if (!need_callback) {
            LOG0 << "RunPushData(): skip PushData as no callback";
            return;
        }

        for (const Child& child : children_)
            child.node->StartPreOp(child.parent_index);
        if (consume_counter() > 0 && consume_counter() != kNeverConsume)
            DecConsumeCounter(1);
        bool consume = context().consume() && consume_counter() == 0;

        PushData(consume);

        if (consume) Dispose();

        for (const Child& child : children_)
            child.node->StopPreOp(child.parent_index);
    }

    //! Method for derived classes to Push a single item to all children.
    void PushItem(const ValueType& item) const {
        for (const Child& child : children_) {
            
            if (child.callback){
                child.callback(item);
            }
           
        }
    }

    //! Method for derived classes to Push a whole File of ValueType items to
    //! all children.
    void PushFile(data::File& file, bool consume) const {
        // iterate over children, push directly into those with data:File*
        std::vector<Child> nonfile_children;
        for (const Child& child : children_) {
            if (child.node->OnPreOpFile(file, child.parent_index))
                LOG0 << "PushFile: direct push accepted by " << *child.node;
            else
                nonfile_children.push_back(child);
        }

        if (nonfile_children.size() == 0) return;

        // push into remaining which have a function stack or no direct File*
        data::File::Reader reader = file.GetReader(consume);
        while (reader.HasNext()) {
            ValueType item = reader.Next<ValueType>();
            for (const Child& child : nonfile_children) {
                if (child.callback)
                    child.callback(item);
            }
        }
    }

protected:
    //! Callback functions from the child nodes.
    std::vector<Child> children_;
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_NODE_HEADER

/******************************************************************************/
