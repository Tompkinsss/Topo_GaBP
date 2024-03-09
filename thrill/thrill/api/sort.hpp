/*******************************************************************************
 * thrill/api/sort.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Michael Axtmann <michael.axtmann@kit.edu>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SORT_HEADER
#define THRILL_API_SORT_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/qsort.hpp>
#include <thrill/common/reservoir_sampling.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/group.hpp>

#include <tlx/math/integer_log2.hpp>
#include <tlx/vector_free.hpp>

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <functional>
#include <numeric>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a Sort operation. Sort sorts a DIA according to a
 * given compare function
 *
 * \tparam ValueType Type of DIA elements
 *
 * \tparam CompareFunction Type of the compare function
 *
 * \tparam SortAlgorithm Type of the local sort function
 *
 * \tparam Stable Whether or not to use stable sorting mechanisms
 *
 * \ingroup api_layer
 */
template <
    typename ValueType,
    typename CompareFunction,
    typename SortAlgorithm,
    bool Stable = false>
class SortNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    //! Set this variable to true to enable generation and output of stats
    static constexpr bool stats_enabled = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    //! Timer or FakeTimer
    using Timer = common::StatsTimerBaseStopped<stats_enabled>;
    //! RIAA class for running the timer
    using RunTimer = common::RunTimer<Timer>;

    using SampleIndexPair = std::pair<ValueType, size_t>;

    //! Stream type for item transmission depends on Stable flag
    using TranmissionStreamType = typename std::conditional<
        Stable,
        data::CatStream, data::MixStream>::type;
    using TranmissionStreamPtr = tlx::CountingPtr<TranmissionStreamType>;

    //! Multiway merge tree creation depends on Stable flag
    struct MakeDefaultMultiwayMergeTree {
        template <typename ReaderIterator,
                  typename Comparator = std::less<ValueType> >
        auto operator () (
            ReaderIterator seqs_begin, ReaderIterator seqs_end,
            const Comparator& comp = Comparator()) {

            return core::make_multiway_merge_tree<ValueType>(
                seqs_begin, seqs_end, comp);
        }
    };

    struct MakeStableMultiwayMergeTree {
        template <typename ReaderIterator,
                  typename Comparator = std::less<ValueType> >
        auto operator () (
            ReaderIterator seqs_begin, ReaderIterator seqs_end,
            const Comparator& comp = Comparator()) {

            return core::make_stable_multiway_merge_tree<ValueType>(
                seqs_begin, seqs_end, comp);
        }
    };

    using MakeMultiwayMergeTree = typename std::conditional<
        Stable,
        MakeStableMultiwayMergeTree, MakeDefaultMultiwayMergeTree>::type;

    static const bool use_background_thread_ = false;

public:
    /*!
     * Constructor for a sort node.
     */
    template <typename ParentDIA>
    SortNode(const ParentDIA& parent,
             const CompareFunction& compare_function,
             const SortAlgorithm& sort_algorithm = SortAlgorithm())
        : Super(parent.ctx(), "Sort", { parent.id() }, { parent.node() }),
          compare_function_(compare_function),
          sort_algorithm_(sort_algorithm),
          parent_stack_empty_(ParentDIA::stack_empty) {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* parent_index */) final {
        timer_preop_.Start();
        unsorted_writer_ = unsorted_file_.GetWriter();
    }

    void PreOp(const ValueType& input) {
        unsorted_writer_.Put(input);
        res_sampler_.add(SampleIndexPair(input, local_items_));
        local_items_++;
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "Sort rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }

        // accept file
        unsorted_file_ = file.Copy();
        local_items_ = unsorted_file_.num_items();

        size_t pick_items = std::min(local_items_, wanted_sample_size());

        sLOG << "Pick" << pick_items << "samples by random access"
             << " from File containing " << local_items_ << " items.";
        for (size_t i = 0; i < pick_items; ++i) {
            size_t index = context_.rng_() % local_items_;
            sLOG << "got index[" << i << "] = " << index;
            samples_.emplace_back(
                unsorted_file_.GetItemAt<ValueType>(index), index);
        }

        return true;
    }

    void StopPreOp(size_t /* parent_index */) final {
        unsorted_writer_.Close();

        LOG0 << "wanted_sample_size()=" << wanted_sample_size()
             << " samples.size()= " << samples_.size();

        timer_preop_.Stop();
        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "Sort() timer_preop_", timer_preop_.SecondsDouble());
            context_.PrintCollectiveMeanStdev(
                "Sort() preop local_items_", local_items_);
        }
    }

    DIAMemUse ExecuteMemUse() final {
        return DIAMemUse::Max();
    }

    //! Executes the sum operation.
    void Execute() final {
        MainOp();
        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "Sort() timer_execute", timer_execute_.SecondsDouble());
        }
    }

    DIAMemUse PushDataMemUse() final {
        if (files_.size() <= 1) {
            // direct push, no merge necessary
            return 0;
        }
        else {
            // need to perform multiway merging
            return DIAMemUse::Max();
        }
    }

    void PushData(bool consume) final {
        Timer timer_pushdata;
        timer_pushdata.Start();

        size_t local_size = 0;
        if (files_.size() == 0) {
            // nothing to push
        }
        else if (files_.size() == 1) {
            local_size = files_[0].num_items();
            this->PushFile(files_[0], consume);
        }
        else {
            size_t merge_degree, prefetch;

            // merge batches of files if necessary
            while (std::tie(merge_degree, prefetch) =
                       context_.block_pool().MaxMergeDegreePrefetch(files_.size()),
                   files_.size() > merge_degree)
            {
                PartialMultiwayMerge(merge_degree, prefetch);
            }

            sLOGC(context_.my_rank() == 0)
                << "Start multi-way-merge of" << files_.size() << "files"
                << "with prefetch" << prefetch;

            // construct output merger of remaining Files
            std::vector<data::File::Reader> seq;
            seq.reserve(files_.size());

            for (size_t t = 0; t < files_.size(); ++t) {
                seq.emplace_back(
                    files_[t].GetReader(consume, /* prefetch */ 0));
            }

            StartPrefetch(seq, prefetch);

            auto puller = MakeMultiwayMergeTree()(
                seq.begin(), seq.end(), compare_function_);

            while (puller.HasNext()) {
                this->PushItem(puller.Next());
                local_size++;
            }
        }

        timer_pushdata.Stop();

        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "Sort() timer_pushdata", timer_pushdata.SecondsDouble());

            context_.PrintCollectiveMeanStdev("Sort() local_size", local_size);
        }
    }

    void Dispose() final {
        files_.clear();
    }

private:
    //! The comparison function which is applied to two elements.
    CompareFunction compare_function_;

    //! Sort function class
    SortAlgorithm sort_algorithm_;

    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

    //! \name PreOp Phase
    //! \{

    //! All local unsorted items before communication
    data::File unsorted_file_ { context_.GetFile(this) };
    //! Writer for unsorted_file_
    data::File::Writer unsorted_writer_;
    //! Number of items on this worker
    size_t local_items_ = 0;

    //! epsilon
    static constexpr double desired_imbalance_ = 0.1;

    //! Sample vector: pairs of (sample,local index)
    std::vector<SampleIndexPair> samples_;
    //! Reservoir sampler
    common::ReservoirSamplingGrow<SampleIndexPair> res_sampler_ {
        samples_, context_.rng_, desired_imbalance_
    };
    //! calculate currently desired number of samples
    size_t wanted_sample_size() const {
        return res_sampler_.calc_sample_size(local_items_);
    }

    //! \}

    //! \name MainOp and PushData
    //! \{

    //! Local data files
    std::vector<data::File> files_;
    //! Total number of local elements after communication
    size_t local_out_size_ = 0;

    //! \}

    //! \name Statistics
    //! \{

    //! time spent in PreOp (including preceding Node's computation)
    Timer timer_preop_;

    //! time spent in Execute
    Timer timer_execute_;

    //! time spent in sort()
    Timer timer_sort_;

    //! \}

    void FindAndSendSplitters(
        std::vector<SampleIndexPair>& splitters, size_t sample_size,
        data::MixStreamPtr& sample_stream,
        data::MixStream::Writers& sample_writers) {

        // Get samples from other workers
        size_t num_total_workers = context_.num_workers();

        std::vector<SampleIndexPair> samples;
        samples.reserve(sample_size * num_total_workers);

        auto reader = sample_stream->GetMixReader(/* consume */ true);

        while (reader.HasNext()) {
            samples.push_back(reader.template Next<SampleIndexPair>());
        }
        if (samples.size() == 0) return;

        LOG << "FindAndSendSplitters() samples.size()=" << samples.size();

        // Find splitters
        std::sort(samples.begin(), samples.end(),
                  [this](
                      const SampleIndexPair& a, const SampleIndexPair& b) {
                      return LessSampleIndex(a, b);
                  });

        double splitting_size = static_cast<double>(samples.size())
                                / static_cast<double>(num_total_workers);

        // Send splitters to other workers
        for (size_t i = 1; i < num_total_workers; ++i) {
            splitters.push_back(
                samples[static_cast<size_t>(i * splitting_size)]);
            for (size_t j = 1; j < num_total_workers; j++) {
                sample_writers[j].Put(splitters.back());
            }
        }

        for (size_t j = 1; j < num_total_workers; ++j)
            sample_writers[j].Close();
    }

    class TreeBuilder
    {
    public:
        ValueType* tree_;
        const SampleIndexPair* samples_;
        size_t index_ = 0;
        size_t ssplitter_;

        /*!
         * Target: tree. Size of 'number of splitter'
         * Source: sorted splitters. Size of 'number of splitter'
         * Number of splitter
         */
        TreeBuilder(ValueType* splitter_tree,
                    const SampleIndexPair* samples,
                    size_t ssplitter)
            : tree_(splitter_tree),
              samples_(samples),
              ssplitter_(ssplitter) {
            if (ssplitter != 0)
                recurse(samples, samples + ssplitter, 1);
        }

        void recurse(const SampleIndexPair* lo, const SampleIndexPair* hi,
                     unsigned int treeidx) {
            // pick middle element as splitter
            const SampleIndexPair* mid = lo + (ssize_t)(hi - lo) / 2;
            assert(mid < samples_ + ssplitter_);
            tree_[treeidx] = mid->first;

            if (2 * treeidx < ssplitter_)
            {
                const SampleIndexPair* midlo = mid, * midhi = mid + 1;
                recurse(lo, midlo, 2 * treeidx + 0);
                recurse(midhi, hi, 2 * treeidx + 1);
            }
        }
    };

    bool LessSampleIndex(const SampleIndexPair& a, const SampleIndexPair& b) {
        return compare_function_(a.first, b.first) || (
            !compare_function_(b.first, a.first) && a.second < b.second);
    }

    bool EqualSampleGreaterIndex(const SampleIndexPair& a, const SampleIndexPair& b) {
        return !compare_function_(a.first, b.first) && a.second >= b.second;
    }

    //! round n down by k where k is a power of two.
    template <typename Integral>
    static inline size_t RoundDown(Integral n, Integral k) {
        return (n & ~(k - 1));
    }

    void TransmitItems(
        // Tree of splitters, sizeof |splitter|
        const ValueType* const tree,
        // Number of buckets: k = 2^{log_k}
        size_t k,
        size_t log_k,
        // Number of actual workers to send to
        size_t actual_k,
        const SampleIndexPair* const sorted_splitters,
        size_t prefix_items,
        TranmissionStreamPtr& data_stream) {

        data::File::ConsumeReader unsorted_reader =
            unsorted_file_.GetConsumeReader();

        auto data_writers = data_stream->GetWriters();

        // enlarge emitters array to next power of two to have direct access,
        // because we fill the splitter set up with sentinels == last splitter,
        // hence all items land in the last bucket.
        assert(data_writers.size() == actual_k);
        assert(actual_k <= k);

        data_writers.reserve(k);
        while (data_writers.size() < k)
            data_writers.emplace_back(typename TranmissionStreamType::Writer());

        std::swap(data_writers[actual_k - 1], data_writers[k - 1]);

        // classify all items (take two at once) and immediately transmit them.

        const size_t stepsize = 2;

        size_t i = prefix_items;
        for ( ; i < prefix_items + RoundDown(local_items_, stepsize); i += stepsize)
        {
            // take two items
            size_t j0 = 1;
            ValueType el0 = unsorted_reader.Next<ValueType>();

            size_t j1 = 1;
            ValueType el1 = unsorted_reader.Next<ValueType>();

            // run items down the tree
            for (size_t l = 0; l < log_k; l++)
            {
                j0 = 2 * j0 + (compare_function_(el0, tree[j0]) ? 0 : 1);
                j1 = 2 * j1 + (compare_function_(el1, tree[j1]) ? 0 : 1);
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            while (b0 && EqualSampleGreaterIndex(
                       sorted_splitters[b0 - 1], SampleIndexPair(el0, i + 0))) {
                b0--;

                // LOG0 << "el0 equal match b0 " << b0
                //      << " prefix_items " << prefix_items
                //      << " lhs.first = " << sorted_splitters[b0 - 1].first
                //      << " lhs.second = " << sorted_splitters[b0 - 1].second
                //      << " rhs.first = " << el0
                //      << " rhs.second = " << i;
            }

            while (b1 && EqualSampleGreaterIndex(
                       sorted_splitters[b1 - 1], SampleIndexPair(el1, i + 1))) {
                b1--;
            }

            assert(data_writers[b0].IsValid());
            assert(data_writers[b1].IsValid());

            data_writers[b0].Put(el0);
            data_writers[b1].Put(el1);
        }

        // last iteration of loop if we have an odd number of items.
        for ( ; i < prefix_items + local_items_; i++)
        {
            size_t j0 = 1;
            ValueType el0 = unsorted_reader.Next<ValueType>();

            // run item down the tree
            for (size_t l = 0; l < log_k; l++)
            {
                j0 = 2 * j0 + (compare_function_(el0, tree[j0]) ? 0 : 1);
            }

            size_t b0 = j0 - k;

            while (b0 && EqualSampleGreaterIndex(
                       sorted_splitters[b0 - 1], SampleIndexPair(el0, i))) {
                b0--;
            }

            assert(data_writers[b0].IsValid());
            data_writers[b0].Put(el0);
        }

        // implicitly close writers and flush data
    }

    void MainOp() {
        RunTimer timer(timer_execute_);

        size_t prefix_items = local_items_;
        size_t total_items = context_.net.ExPrefixSumTotal(prefix_items);

        size_t num_total_workers = context_.num_workers();

        sLOG << "worker" << context_.my_rank()
             << "local_items_" << local_items_
             << "prefix_items" << prefix_items
             << "total_items" << total_items
             << "local sample_.size()" << samples_.size();

        if (total_items == 0) {
            Super::logger_
                << "class" << "SortNode"
                << "event" << "done"
                << "workers" << num_total_workers
                << "local_out_size" << local_out_size_
                << "balance" << 0
                << "sample_size" << samples_.size();
            return;
        }

        // stream to send samples to process 0 and receive them back
        data::MixStreamPtr sample_stream = context_.GetNewMixStream(this);

        // Send all samples to worker 0.
        data::MixStream::Writers sample_writers = sample_stream->GetWriters();

        for (const SampleIndexPair& sample : samples_) {
            // send samples but add the local prefix to index ranks
            sample_writers[0].Put(
                SampleIndexPair(sample.first, prefix_items + sample.second));
        }
        sample_writers[0].Close();
        tlx::vector_free(samples_);

        // Get the ceiling of log(num_total_workers), as SSSS needs 2^n buckets.
        size_t ceil_log = tlx::integer_log2_ceil(num_total_workers);
        size_t workers_algo = size_t(1) << ceil_log;
        size_t splitter_count_algo = workers_algo - 1;

        std::vector<SampleIndexPair> splitters;
        splitters.reserve(workers_algo);

        if (context_.my_rank() == 0) {
            FindAndSendSplitters(splitters, samples_.size(),
                                 sample_stream, sample_writers);
        }
        else {
            // Close unused emitters
            for (size_t j = 1; j < num_total_workers; j++) {
                sample_writers[j].Close();
            }
            data::MixStream::MixReader reader =
                sample_stream->GetMixReader(/* consume */ true);
            while (reader.HasNext()) {
                splitters.push_back(reader.template Next<SampleIndexPair>());
            }
        }
        sample_writers.clear();
        sample_stream.reset();

        // code from SS2NPartition, slightly altered

        std::vector<ValueType> splitter_tree(workers_algo + 1);

        // add sentinel splitters if fewer nodes than splitters.
        for (size_t i = num_total_workers; i < workers_algo; i++) {
            splitters.push_back(splitters.back());
        }

        TreeBuilder(splitter_tree.data(),
                    splitters.data(),
                    splitter_count_algo);

        auto data_stream = context_.template GetNewStream<TranmissionStreamType>(this->dia_id());

        std::thread thread;
        if (use_background_thread_) {
            // launch receiver thread.
            thread = common::CreateThread(
                [this, &data_stream]() {
                    common::SetCpuAffinity(context_.local_worker_id());
                    return ReceiveItems(data_stream);
                });
        }

        TransmitItems(
            splitter_tree.data(), // Tree. sizeof |splitter|
            workers_algo,         // Number of buckets
            ceil_log,
            num_total_workers,
            splitters.data(),
            prefix_items,
            data_stream);

        tlx::vector_free(splitter_tree);

        if (use_background_thread_)
            thread.join();
        else
            ReceiveItems(data_stream);

        data_stream.reset();

        double balance = 0;
        if (local_out_size_ > 0) {
            balance = static_cast<double>(local_out_size_)
                      * static_cast<double>(num_total_workers)
                      / static_cast<double>(total_items);
        }

        if (balance > 1) {
            balance = 1 / balance;
        }

        Super::logger_
            << "class" << "SortNode"
            << "event" << "done"
            << "workers" << num_total_workers
            << "local_out_size" << local_out_size_
            << "balance" << balance
            << "sample_size" << samples_.size();
    }

    void ReceiveItems(TranmissionStreamPtr& data_stream) {

        auto reader = data_stream->GetReader(/* consume */ true);

        LOG0 << "Writing files";

        // M/2 such that the other half is used to prepare the next bulk
        size_t capacity = DIABase::mem_limit_ / sizeof(ValueType) / 2;
        size_t capacity_half = capacity / 2;
        std::vector<ValueType> vec;
        vec.reserve(capacity);

        while (reader.HasNext()) {
            if (vec.size() < capacity_half ||
                (vec.size() < capacity && !mem::memory_exceeded)) {
                vec.push_back(reader.template Next<ValueType>());
            }
            else {
                SortAndWriteToFile(vec);
            }
        }

        if (vec.size())
            SortAndWriteToFile(vec);

        if (stats_enabled) {
            context_.PrintCollectiveMeanStdev(
                "Sort() timer_sort_", timer_sort_.SecondsDouble());
        }
    }

    void SortAndWriteToFile(std::vector<ValueType>& vec) {

        LOG << "SortAndWriteToFile() " << vec.size()
            << " items into file #" << files_.size();

        die_unless(vec.size() > 0);

        size_t vec_size = vec.size();
        local_out_size_ += vec.size();

        // advise block pool to write out data if necessary
        // context_.block_pool().AdviseFree(vec.size() * sizeof(ValueType));

        timer_sort_.Start();
        sort_algorithm_(vec.begin(), vec.end(), compare_function_);
        // common::qsort_two_pivots_yaroslavskiy(vec.begin(), vec.end(), compare_function_);
        // common::qsort_three_pivots(vec.begin(), vec.end(), compare_function_);
        timer_sort_.Stop();

        LOG0 << "SortAndWriteToFile() sort took " << timer_sort_;

        Timer write_time;
        write_time.Start();

        files_.emplace_back(context_.GetFile(this));
        auto writer = files_.back().GetWriter();
        for (const ValueType& elem : vec) {
            writer.Put(elem);
        }
        writer.Close();

        write_time.Stop();

        LOG0 << "SortAndWriteToFile() finished writing files";

        vec.clear();

        LOG0 << "SortAndWriteToFile() vector cleared";

        Super::logger_
            << "class" << "SortNode"
            << "event" << "write_file"
            << "file_num" << (files_.size() - 1)
            << "items" << vec_size
            << "timer_sort_" << timer_sort_
            << "write_time" << write_time;
    }

    void PartialMultiwayMerge(size_t merge_degree, size_t prefetch) {
        sLOG1 << "Partial multi-way-merge of" << files_.size()
              << "files with degree" << merge_degree
              << "and prefetch" << prefetch;

        std::vector<data::File> new_files;

        // merge batches of merge_degree Files into new_files
        size_t fi;
        for (fi = 0; fi + merge_degree < files_.size(); fi += merge_degree) {
            // create merger for first merge_degree Files
            std::vector<data::File::ConsumeReader> seq;
            seq.reserve(merge_degree);

            for (size_t t = 0; t < merge_degree; ++t) {
                seq.emplace_back(
                    files_[fi + t].GetConsumeReader(/* prefetch */ 0));
            }

            StartPrefetch(seq, prefetch);

            auto puller = MakeMultiwayMergeTree()(
                seq.begin(), seq.end(), compare_function_);

            // create new File for merged items
            new_files.emplace_back(context_.GetFile(this));
            auto writer = new_files.back().GetWriter();

            while (puller.HasNext()) {
                writer.Put(puller.Next());
            }
            writer.Close();

            // merged files are cleared by the ConsumeReader
        }

        // copy remaining files into new_files
        for ( ; fi < files_.size(); ++fi) {
            new_files.emplace_back(std::move(files_[fi]));
        }

        std::swap(files_, new_files);
    }
};

class DefaultSortAlgorithm
{
public:
    template <typename Iterator, typename CompareFunction>
    void operator () (Iterator begin, Iterator end, CompareFunction cmp) const {
        return std::sort(begin, end, cmp);
    }
};

template <typename ValueType, typename Stack>
template <typename CompareFunction>
auto DIA<ValueType, Stack>::Sort(const CompareFunction& compare_function) const {
    assert(IsValid());

    using SortNode = api::SortNode<
        ValueType, CompareFunction, DefaultSortAlgorithm>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<0> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<1> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto node = tlx::make_counting<SortNode>(*this, compare_function);

    return DIA<ValueType>(node);
}

template <typename ValueType, typename Stack>
template <typename CompareFunction, typename SortAlgorithm>
auto DIA<ValueType, Stack>::Sort(const CompareFunction& compare_function,
                                 const SortAlgorithm& sort_algorithm) const {
    assert(IsValid());

    using SortNode = api::SortNode<
        ValueType, CompareFunction, SortAlgorithm>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<0> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<1> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto node = tlx::make_counting<SortNode>(
        *this, compare_function, sort_algorithm);

    return DIA<ValueType>(node);
}

class DefaultStableSortAlgorithm
{
public:
    template <typename Iterator, typename CompareFunction>
    void operator () (Iterator begin, Iterator end, CompareFunction cmp) const {
        return std::stable_sort(begin, end, cmp);
    }
};

template <typename ValueType, typename Stack>
template <typename CompareFunction>
auto DIA<ValueType, Stack>::SortStable(
    const CompareFunction& compare_function) const {

    assert(IsValid());

    using SortStableNode = api::SortNode<
        ValueType, CompareFunction, DefaultStableSortAlgorithm, /*Stable*/ true>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<0> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<1> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto node = tlx::make_counting<SortStableNode>(*this, compare_function);

    return DIA<ValueType>(node);
}

template <typename ValueType, typename Stack>
template <typename CompareFunction, typename SortAlgorithm>
auto DIA<ValueType, Stack>::SortStable(
    const CompareFunction& compare_function,
    const SortAlgorithm& sort_algorithm) const {

    assert(IsValid());

    using SortStableNode = api::SortNode<
        ValueType, CompareFunction, SortAlgorithm, /* Stable */ true>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<0> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<CompareFunction>::template arg<1> >::value,
        "CompareFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<CompareFunction>::result_type,
            bool>::value,
        "CompareFunction has the wrong output type (should be bool)");

    auto node = tlx::make_counting<SortStableNode>(
        *this, compare_function, sort_algorithm);

    return DIA<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SORT_HEADER

/******************************************************************************/
