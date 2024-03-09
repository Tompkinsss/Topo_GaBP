/*******************************************************************************
 * thrill/data/byte_block.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BYTE_BLOCK_HEADER
#define THRILL_DATA_BYTE_BLOCK_HEADER

#include <thrill/mem/pool.hpp>

#include <foxxll/io/file.hpp>
#include <foxxll/mng/bid.hpp>
#include <tlx/counting_ptr.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

//! starting size of blocks in BlockWriter.
extern size_t start_block_size;

//! default size of blocks in File, Channel, BlockQueue, etc.
extern size_t default_block_size;

//! type of underlying memory area
using Byte = uint8_t;

// forward declarations.
class BlockPool;

/*!
 * A ByteBlock is the basic storage units of containers like File, BlockQueue,
 * etc. It consists of a fixed number of bytes without any type and meta
 * information. Conceptually a ByteBlock is written _once_ and can then be
 * shared read-only between containers using CountingPtr<const ByteBlock>
 * reference counting inside a Block, which adds meta information.
 *
 * ByteBlocks can be swapped to disk, which decreases their size to 0.
 */
class ByteBlock : public tlx::ReferenceCounter
{
    static constexpr bool debug = false;

public:
    //! deleter for CountingPtr<ByteBlock>
    class Deleter
    {
    public:
        void operator () (ByteBlock* bb) const;
        void operator () (const ByteBlock* bb) const;
    };

    using ByteBlockPtr = tlx::CountingPtr<ByteBlock, Deleter>;
    using ByteBlockCPtr = tlx::CountingPtr<const ByteBlock, Deleter>;

public:
    //! mutable data accessor to memory block
    Byte * data() { return data_; }
    //! const data accessor to memory block
    const Byte * data() const { return data_; }

    //! mutable data accessor to beginning of memory block
    Byte * begin() { return data_; }
    //! const data accessor to beginning of memory block
    const Byte * begin() const { return data_; }

    //! mutable data accessor beyond end of memory block
    Byte * end() { return data_ + size_; }
    //! const data accessor beyond end of memory block
    const Byte * end() const { return data_ + size_; }

    //! the block size
    size_t size() const { return size_; }

    //! Returns whether the ByteBlock is in an external file.
    bool has_ext_file() const { return ext_file_.get() != nullptr; }

    //! return current pin count
    size_t pin_count(size_t local_worker_id) const {
        return pin_count_[local_worker_id];
    }

    //! return string list of pin_counts
    std::string pin_count_str() const;

    //! true if block resides in memory
    bool in_memory() const {
        return data_ != nullptr;
    }

    //! true if being deleted
    bool is_deleted() const {
        return pin_count_.empty();
    }

    //! increment pin count, must be >= 1 before.
    void IncPinCount(size_t local_worker_id);

    //! decrement pin count, possibly signal block pool that if it reaches zero.
    void DecPinCount(size_t local_worker_id);

private:
    //! the memory block itself is referenced as it is in a a separate memory
    //! region that can be swapped out
    Byte* data_;

    //! the allocated size of the buffer in bytes
    const size_t size_;

    //! reference to BlockPool for deletion.
    BlockPool* block_pool_;

    //! counts the number of pins in this block per thread_id.
    std::vector<size_t, mem::GPoolAllocator<size_t> > pin_count_;

    //! counts the total number of pins, the data_ may be swapped out when this
    //! reaches zero.
    size_t total_pins_ = 0;

    //! external memory block, which contains a pointer to foxxll::file, an
    //! offset into the file, and (unfortunately) also the size.
    foxxll::BID<0> em_bid_;

    //! shared pointer to external file, if this is != nullptr then the Block
    //! was created for directly reading binary files.
    foxxll::file_ptr ext_file_;

    // BlockPool is a friend to call ctor and to manipulate data_.
    friend class BlockPool;
    // Block is a friend to call {Increase,Reduce}PinCount()
    friend class Block;
    friend class PinnedBlock;
    // for calling protected constructor
    friend class mem::Pool;

    //! No default construction of Byteblock
    ByteBlock() = delete;

    /*!
     * Constructor to initialize ByteBlock in a buffer of memory. Protected,
     * used BlockPool::AllocateByteBlock() for construction.
     *
     * \param data the memory address of the byte-blocks data. nullptr if swapped out
     * \param size the size of the block in bytes
     * \param block_pool the block pool that manages this ByteBlock
     */
    ByteBlock(BlockPool* block_pool, Byte* data, size_t size);

    //! Constructor to initialize ByteBlock as a mapping to an external
    //! foxxll::file area.
    ByteBlock(BlockPool* block_pool, const foxxll::file_ptr& ext_file,
              int64_t offset, size_t size);

    friend std::ostream& operator << (std::ostream& os, const ByteBlock& b);

    //! forwarded to block_pool_
    void OnWriteComplete(foxxll::request* req, bool success);
};

class PinnedByteBlockPtr;

/*!
 * A non-pinned counting pointer to a ByteBlock
 */
class ByteBlockPtr : public ByteBlock::ByteBlockPtr
{
    using Super = ByteBlock::ByteBlockPtr;

public:
    //! default ctor: returns an empty ByteBlockPtr
    ByteBlockPtr() = default;

protected:
    //! (protected) ctor from PinnedByteBlockPtr
    ByteBlockPtr(const PinnedByteBlockPtr& p);
    //! (protected) ctor from PinnedByteBlockPtr
    ByteBlockPtr(PinnedByteBlockPtr&& p);
    //! (protected) ctor from plain pointer
    ByteBlockPtr(ByteBlock* ptr) : Super(ptr) { }

    friend class PinnedByteBlockPtr;
    friend class BlockPool;
};

/*!
 * A pinned / pin-counted pointer to a ByteBlock. By holding a pin, it is a
 * guaranteed that the ByteBlock's underlying memory is loaded in RAM. Since
 * pins are counted per thread, the PinnedByteBlockPtr is a counting pointer
 * plus a thread id.
 *
 * Be careful to move PinnedByteBlockPtr as must as possible, since copying
 * costs a pinning and an unpinning operation, whereas moving is free.
 */
class PinnedByteBlockPtr : public ByteBlockPtr
{
public:
    //! default ctor: contains a nullptr pointer.
    PinnedByteBlockPtr() noexcept = default;

    //! copy-ctor: increment underlying's pin count
    PinnedByteBlockPtr(const PinnedByteBlockPtr& pbb) noexcept
        : ByteBlockPtr(pbb), local_worker_id_(pbb.local_worker_id_) {
        if (valid()) get()->IncPinCount(local_worker_id_);
    }

    //! move-ctor: move underlying's pin
    PinnedByteBlockPtr(PinnedByteBlockPtr&& pbb) noexcept
        : ByteBlockPtr(std::move(pbb)), local_worker_id_(pbb.local_worker_id_) {
        assert(!pbb.valid());
    }

    //! copy-assignment: transfer underlying's pin count
    PinnedByteBlockPtr& operator = (PinnedByteBlockPtr& pbb) noexcept {
        if (this == &pbb) return *this;
        // first acquire other's pin count
        if (pbb.valid()) pbb->IncPinCount(pbb.local_worker_id_);
        // then release the current one
        if (valid()) get()->DecPinCount(local_worker_id_);
        // copy over information, keep pin
        ByteBlockPtr::operator = (pbb);
        local_worker_id_ = pbb.local_worker_id_;
        return *this;
    }

    //! move-assignment: move underlying's pin
    PinnedByteBlockPtr& operator = (PinnedByteBlockPtr&& pbb) noexcept {
        if (this == &pbb) return *this;
        // release the current one
        if (valid()) get()->DecPinCount(local_worker_id_);
        // move over information, keep other's pin
        ByteBlockPtr::operator = (std::move(pbb));
        local_worker_id_ = pbb.local_worker_id_;
        // invalidated other block
        assert(!pbb.valid());
        return *this;
    }

    //! destructor: remove pin
    ~PinnedByteBlockPtr() {
        if (valid()) get()->DecPinCount(local_worker_id_);
    }

    //! local worker id of holder of pin
    size_t local_worker_id() const { return local_worker_id_; }

    //! relinquish pin
    ByteBlockPtr ReleasePin() && {
        if (valid()) get()->DecPinCount(local_worker_id_);
        return std::move(*this);
    }

private:
    //! protected ctor for calling from Acquire().
    PinnedByteBlockPtr(ByteBlock* ptr, size_t local_worker_id) noexcept
        : ByteBlockPtr(ptr), local_worker_id_(local_worker_id) { }

    //! protected ctor for calling from Acquire().
    PinnedByteBlockPtr(ByteBlockPtr&& ptr, size_t local_worker_id) noexcept
        : ByteBlockPtr(std::move(ptr)), local_worker_id_(local_worker_id) { }

    //! local worker id of holder of pin
    size_t local_worker_id_;

    //! for access to protected constructor to transfer pin
    friend class PinnedBlock;
    //! for access to protected constructor to AllocateByteBlock().
    friend class BlockPool;
};

inline
ByteBlockPtr::ByteBlockPtr(const PinnedByteBlockPtr& p) : Super(p) { }

inline
ByteBlockPtr::ByteBlockPtr(PinnedByteBlockPtr&& p) : Super(std::move(p)) { }

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BYTE_BLOCK_HEADER

/******************************************************************************/
